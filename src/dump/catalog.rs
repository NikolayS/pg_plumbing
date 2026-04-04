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
    /// Owner role name.
    pub owner: String,
    /// Columns in ordinal order.
    pub columns: Vec<ColumnInfo>,
    /// Primary key constraint, if any.
    pub primary_key: Option<ConstraintInfo>,
    /// All constraints (CHECK, UNIQUE, FOREIGN KEY, NOT NULL, PRIMARY KEY).
    /// Used to emit `ALTER TABLE … ADD CONSTRAINT` statements after CREATE TABLE.
    pub constraints: Vec<ConstraintInfo>,
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
    /// Constraint type: 'c'=CHECK, 'u'=UNIQUE, 'f'=FOREIGN KEY, 'n'=NOT NULL, 'p'=PRIMARY KEY.
    pub contype: char,
    /// Whether the constraint is deferrable.
    pub deferrable: bool,
    /// Whether the constraint is initially deferred.
    pub deferred: bool,
}

impl TableInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a single sequence.
#[derive(Debug, Clone)]
pub struct SequenceInfo {
    /// Schema name.
    pub schema: String,
    /// Sequence name.
    pub name: String,
    /// Start value.
    pub start_value: i64,
    /// Increment.
    pub increment_by: i64,
    /// Minimum value.
    pub min_value: i64,
    /// Maximum value.
    pub max_value: i64,
    /// Cache size.
    pub cache_size: i64,
    /// Whether the sequence cycles.
    pub cycle: bool,
    /// OWNED BY table schema (if owned by a column).
    pub owned_by_schema: Option<String>,
    /// OWNED BY table name (if owned by a column).
    pub owned_by_table: Option<String>,
    /// OWNED BY column name (if owned by a column).
    pub owned_by_column: Option<String>,
}

impl SequenceInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a single view.
#[derive(Debug, Clone)]
pub struct ViewInfo {
    /// Schema name.
    pub schema: String,
    /// View name.
    pub name: String,
    /// View definition from `pg_get_viewdef`.
    pub definition: String,
}

impl ViewInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a single schema.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// Schema name.
    pub name: String,
    /// Owner role name.
    pub owner: String,
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
                        r.rolname as owner,
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
                 join pg_catalog.pg_roles r on r.oid = c.relowner
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
                            r.rolname as owner,
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
                     join pg_catalog.pg_roles r on r.oid = c.relowner
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
        let owner: &str = row.get("owner");
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
        let constraints = get_constraints(client, oid).await?;
        // Keep backward-compat field pointing at the first PRIMARY KEY found.
        let primary_key = constraints.iter().find(|c| c.contype == 'p').cloned();

        tables.push(TableInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            owner: owner.to_string(),
            columns,
            primary_key,
            constraints,
            partition_key: partition_key.filter(|s| !s.is_empty()),
            partition_bound: partition_bound.filter(|s| !s.is_empty()),
            parent_table,
            parent_schema,
        });
    }

    Ok(tables)
}

/// Query user-visible schemas (excludes system schemas) with their owners.
pub async fn get_schemas(client: &Client, opts: &DumpOptions) -> Result<Vec<SchemaInfo>> {
    let rows = client
        .query(
            "SELECT n.nspname, r.rolname AS owner
             FROM pg_catalog.pg_namespace n
             JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
             WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
               AND n.nspname NOT LIKE 'pg_temp_%'
               AND n.nspname NOT LIKE 'pg_toast_temp_%'
             ORDER BY n.nspname",
            &[],
        )
        .await
        .context("query schemas")?;

    let mut schemas = Vec::new();
    for row in &rows {
        let name: &str = row.get("nspname");
        let owner: &str = row.get("owner");

        // Apply schema inclusion/exclusion filters.
        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, name) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, name) {
            continue;
        }

        schemas.push(SchemaInfo {
            name: name.to_string(),
            owner: owner.to_string(),
        });
    }
    Ok(schemas)
}

/// Query the catalog for sequences matching the dump options.
///
/// Returns sequences from non-system schemas, ordered by schema then name.
/// Each sequence includes OWNED BY information (table/column) if applicable.
pub async fn get_sequences(client: &Client, opts: &DumpOptions) -> Result<Vec<SequenceInfo>> {
    let mut excluded = vec!["pg_catalog".to_string(), "information_schema".to_string()];
    let literal_excludes: Vec<String> = opts
        .exclude_schemas
        .iter()
        .filter(|p| !p.contains(['*', '?']))
        .cloned()
        .collect();
    excluded.extend(literal_excludes);

    let rows = client
        .query(
            "SELECT n.nspname AS schema_name,
                    s.relname AS seq_name,
                    seq.seqstart AS start_value,
                    seq.seqincrement AS increment_by,
                    seq.seqmin AS min_value,
                    seq.seqmax AS max_value,
                    seq.seqcache AS cache_size,
                    seq.seqcycle AS cycle,
                    t.relname AS owned_by_table,
                    tn.nspname AS owned_by_schema,
                    a.attname AS owned_by_column
             FROM pg_catalog.pg_class s
             JOIN pg_catalog.pg_namespace n ON n.oid = s.relnamespace
             JOIN pg_catalog.pg_sequence seq ON seq.seqrelid = s.oid
             LEFT JOIN pg_catalog.pg_depend d
               ON d.objid = s.oid
              AND d.classid = 'pg_catalog.pg_class'::regclass
              AND d.refclassid = 'pg_catalog.pg_class'::regclass
              AND d.deptype = 'a'
             LEFT JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
             LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
             LEFT JOIN pg_catalog.pg_attribute a
               ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
             WHERE s.relkind = 'S'
               AND n.nspname != all($1)
             ORDER BY n.nspname, s.relname",
            &[&excluded],
        )
        .await
        .context("query sequences")?;

    let mut sequences = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        // Schema inclusion filter (supports glob patterns).
        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        // Schema exclusion filter (supports glob patterns).
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        sequences.push(SequenceInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("seq_name").to_string(),
            start_value: row.get("start_value"),
            increment_by: row.get("increment_by"),
            min_value: row.get("min_value"),
            max_value: row.get("max_value"),
            cache_size: row.get("cache_size"),
            cycle: row.get("cycle"),
            owned_by_schema: row.get("owned_by_schema"),
            owned_by_table: row.get("owned_by_table"),
            owned_by_column: row.get("owned_by_column"),
        });
    }

    Ok(sequences)
}

/// Query the catalog for views matching the dump options.
///
/// Returns views from non-system schemas, ordered by schema then name.
pub async fn get_views(client: &Client, opts: &DumpOptions) -> Result<Vec<ViewInfo>> {
    let mut excluded = vec!["pg_catalog".to_string(), "information_schema".to_string()];
    let literal_excludes: Vec<String> = opts
        .exclude_schemas
        .iter()
        .filter(|p| !p.contains(['*', '?']))
        .cloned()
        .collect();
    excluded.extend(literal_excludes);

    let rows = client
        .query(
            "SELECT n.nspname AS schema_name,
                    c.relname AS view_name,
                    pg_catalog.pg_get_viewdef(c.oid, true) AS definition
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind = 'v'
               AND n.nspname != all($1)
             ORDER BY n.nspname, c.relname",
            &[&excluded],
        )
        .await
        .context("query views")?;

    let mut views = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        // Schema inclusion filter (supports glob patterns).
        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        // Schema exclusion filter (supports glob patterns).
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let definition: Option<String> = row.get("definition");
        views.push(ViewInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("view_name").to_string(),
            definition: definition.unwrap_or_default(),
        });
    }

    Ok(views)
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

/// Query all constraints for a table by OID.
///
/// Returns constraints of type: CHECK ('c'), UNIQUE ('u'), FOREIGN KEY ('f'),
/// NOT NULL ('n'), and PRIMARY KEY ('p'), ordered by type then name.
///
/// Constraints inherited from a parent (coninhcount > 0 and !conislocal) are
/// excluded — they will be emitted for the parent table only.
pub async fn get_constraints(client: &Client, table_oid: u32) -> Result<Vec<ConstraintInfo>> {
    let oid_i64 = table_oid as i64;
    let rows = client
        .query(
            "SELECT conname, \
                    contype::text AS contype, \
                    pg_catalog.pg_get_constraintdef(c.oid, true) AS condef, \
                    condeferrable, \
                    condeferred \
             FROM pg_catalog.pg_constraint c \
             WHERE c.conrelid = $1::bigint::oid \
               AND c.contype IN ('c', 'u', 'f', 'n', 'p') \
               AND (c.conislocal OR c.coninhcount = 0) \
             ORDER BY contype, conname",
            &[&oid_i64],
        )
        .await
        .context("query constraints")?;

    let mut constraints = Vec::new();
    for row in &rows {
        // contype was cast to text in the query; extract first char.
        let contype_str: &str = row.get("contype");
        let contype = contype_str.chars().next().unwrap_or('c');
        constraints.push(ConstraintInfo {
            name: row.get("conname"),
            definition: row.get("condef"),
            contype,
            deferrable: row.get("condeferrable"),
            deferred: row.get("condeferred"),
        });
    }
    Ok(constraints)
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
