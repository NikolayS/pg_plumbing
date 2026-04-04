// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! PostgreSQL catalog queries for schema introspection.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::DumpOptions;

/// Metadata for a single table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Schema name (e.g. `public`).
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Columns in ordinal order.
    pub columns: Vec<ColumnInfo>,
    /// Primary key constraint, if any.
    pub primary_key: Option<ConstraintInfo>,
    /// Partition key expression (e.g. `HASH (mod)`) — Some for partitioned tables.
    pub partition_key: Option<String>,
    /// Partition bound expression (e.g. `FOR VALUES WITH (MODULUS 3, REMAINDER 0)`)
    /// — Some for partition child tables.
    pub partition_bound: Option<String>,
    /// Name of the parent partitioned table — Some for partition children.
    pub parent_table: Option<String>,
    /// Schema of the parent partitioned table — Some for partition children.
    pub parent_schema: Option<String>,
}

/// Metadata for a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Full type name including modifiers (e.g. `character varying(100)`).
    pub type_name: String,
    /// Whether the column has a NOT NULL constraint.
    pub not_null: bool,
    /// Default expression, if any.
    pub default_expr: Option<String>,
}

/// Metadata for a constraint.
#[derive(Debug, Clone)]
pub struct ConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Constraint definition (e.g. `PRIMARY KEY (id)`).
    pub definition: String,
}

impl TableInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Query the catalog for tables matching the dump options.
///
/// Returns both regular tables (`relkind = 'r'`) and partitioned tables
/// (`relkind = 'p'`), ordered so that parent tables precede their children.
pub async fn get_tables(client: &Client, opts: &DumpOptions) -> Result<Vec<TableInfo>> {
    let table_rows = if opts.tables.is_empty() {
        // Dump all user tables and partitioned tables (no system schemas).
        let mut excluded = vec!["pg_catalog".to_string(), "information_schema".to_string()];
        // Only push literal (non-glob) patterns into the SQL != ALL() list.
        // Glob patterns are applied in-memory below via schema_matches_any.
        let literal_excludes: Vec<String> = opts
            .exclude_schemas
            .iter()
            .filter(|p| !p.contains(['*', '?']))
            .cloned()
            .collect();
        excluded.extend(literal_excludes);

        client
            .query(
                "select c.oid::int8 as oid,
                        n.nspname,
                        c.relname,
                        c.relkind,
                        pg_catalog.pg_get_partkeydef(c.oid) as partition_key,
                        pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_bound,
                        (SELECT p.relname
                         FROM pg_catalog.pg_inherits i
                         JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
                         WHERE i.inhrelid = c.oid
                         LIMIT 1) AS parent_table,
                        (SELECT pn.nspname
                         FROM pg_catalog.pg_inherits i
                         JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
                         JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE i.inhrelid = c.oid
                         LIMIT 1) AS parent_schema
                 from pg_catalog.pg_class c
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                 where c.relkind in ('r', 'p')
                   and n.nspname != all($1)
                 order by
                   -- Parent tables (partition_bound IS NULL) before children.
                   (pg_catalog.pg_get_expr(c.relpartbound, c.oid) IS NOT NULL),
                   n.nspname,
                   c.relname",
                &[&excluded],
            )
            .await
            .context("query tables")?
    } else {
        // Dump only specified tables.
        let mut rows = Vec::new();
        for tbl in &opts.tables {
            let (schema, name) = parse_qualified_name(tbl);
            let result = client
                .query(
                    "select c.oid::int8 as oid,
                            n.nspname,
                            c.relname,
                            c.relkind,
                            pg_catalog.pg_get_partkeydef(c.oid) as partition_key,
                            pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_bound,
                            (SELECT p.relname
                             FROM pg_catalog.pg_inherits i
                             JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
                             WHERE i.inhrelid = c.oid
                             LIMIT 1) AS parent_table,
                            (SELECT pn.nspname
                             FROM pg_catalog.pg_inherits i
                             JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
                             JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace
                             WHERE i.inhrelid = c.oid
                             LIMIT 1) AS parent_schema
                     from pg_catalog.pg_class c
                     join pg_catalog.pg_namespace n
                       on n.oid = c.relnamespace
                     where c.relkind in ('r', 'p')
                       and n.nspname = $1
                       and c.relname = $2
                     order by n.nspname, c.relname",
                    &[&schema, &name],
                )
                .await
                .with_context(|| format!("query table {schema}.{name}"))?;
            rows.extend(result);
        }
        rows
    };

    // Apply schema and table filters.
    let mut tables = Vec::new();
    for row in &table_rows {
        let oid: u32 = row.get::<_, i64>("oid") as u32;
        let schema: &str = row.get("nspname");
        let name: &str = row.get("relname");
        let partition_key: Option<String> = row.get("partition_key");
        let partition_bound: Option<String> = row.get("partition_bound");
        let parent_table: Option<String> = row.get("parent_table");
        let parent_schema: Option<String> = row.get("parent_schema");

        // Schema inclusion filter (supports glob patterns).
        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        // Schema exclusion filter (supports glob patterns).
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }
        // Table exclusion filter (supports glob patterns).
        if super::filter::matches_any(&opts.exclude_tables, schema, name) {
            continue;
        }

        // Query columns for all tables — used both for DDL (regular/partitioned
        // parents) and for data dumping (partition children).
        let columns = get_columns(client, oid).await?;
        let primary_key = get_primary_key(client, oid).await?;

        tables.push(TableInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            columns,
            primary_key,
            partition_key: partition_key.filter(|s| !s.is_empty()),
            partition_bound: partition_bound.filter(|s| !s.is_empty()),
            parent_table,
            parent_schema,
        });
    }

    Ok(tables)
}

/// Parse a potentially qualified table name into (schema, name).
fn parse_qualified_name(input: &str) -> (String, String) {
    if let Some((schema, name)) = input.split_once('.') {
        (schema.to_string(), name.to_string())
    } else {
        ("public".to_string(), input.to_string())
    }
}

/// Query columns for a table by OID.
async fn get_columns(client: &Client, table_oid: u32) -> Result<Vec<ColumnInfo>> {
    let oid_i64 = table_oid as i64;
    let rows = client
        .query(
            "select a.attname, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name, \
                    a.attnotnull, \
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) as default_expr \
             from pg_catalog.pg_attribute a \
             left join pg_catalog.pg_attrdef d \
               on d.adrelid = a.attrelid and d.adnum = a.attnum \
             where a.attrelid = $1::bigint::oid \
               and a.attnum > 0 \
               and not a.attisdropped \
             order by a.attnum",
            &[&oid_i64],
        )
        .await
        .context("query columns")?;

    let mut columns = Vec::new();
    for row in &rows {
        columns.push(ColumnInfo {
            name: row.get("attname"),
            type_name: row.get("type_name"),
            not_null: row.get("attnotnull"),
            default_expr: row.get("default_expr"),
        });
    }
    Ok(columns)
}

/// Query primary key constraint for a table by OID.
async fn get_primary_key(client: &Client, table_oid: u32) -> Result<Option<ConstraintInfo>> {
    let oid_i64 = table_oid as i64;
    let rows = client
        .query(
            "select conname, \
                    pg_catalog.pg_get_constraintdef(c.oid) as condef \
             from pg_catalog.pg_constraint c \
             where c.conrelid = $1::bigint::oid \
               and c.contype = 'p'",
            &[&oid_i64],
        )
        .await
        .context("query primary key")?;

    Ok(rows.first().map(|row| ConstraintInfo {
        name: row.get("conname"),
        definition: row.get("condef"),
    }))
}

/// Quote an SQL identifier if it needs quoting.
pub fn quote_ident(name: &str) -> String {
    // Simple heuristic: quote if not all lowercase alphanumeric/underscore,
    // or if it's a reserved word.
    let needs_quoting = name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        || name.chars().next().is_some_and(|c| c.is_ascii_digit());

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("foo"), "foo");
        assert_eq!(quote_ident("foo_bar"), "foo_bar");
    }

    #[test]
    fn quote_ident_needs_quoting() {
        assert_eq!(quote_ident("Foo"), "\"Foo\"");
        assert_eq!(quote_ident("foo bar"), "\"foo bar\"");
        assert_eq!(quote_ident("123"), "\"123\"");
    }

    #[test]
    fn parse_qualified_simple() {
        let (s, n) = parse_qualified_name("myschema.mytable");
        assert_eq!(s, "myschema");
        assert_eq!(n, "mytable");
    }

    #[test]
    fn parse_qualified_default_schema() {
        let (s, n) = parse_qualified_name("mytable");
        assert_eq!(s, "public");
        assert_eq!(n, "mytable");
    }
}
