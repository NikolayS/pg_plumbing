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

/// Metadata for a privilege statement.
#[derive(Debug, Clone)]
pub struct PrivilegeInfo {
    /// The privilege statement (e.g., `GRANT SELECT ON TABLE public.foo TO PUBLIC;`).
    pub statement: String,
}

/// Parse a PostgreSQL ACL entry (`"grantee=privs/grantor"`) into (grantee_name, privileges).
///
/// An empty grantee means the `PUBLIC` pseudo-role.
pub fn parse_acl_entry(entry: &str) -> Option<(String, Vec<String>)> {
    let eq_pos = entry.find('=')?;
    let grantee = &entry[..eq_pos];
    let after_eq = &entry[eq_pos + 1..];
    let slash_pos = after_eq.find('/')?;
    let privs_str = &after_eq[..slash_pos];

    let grantee_name = if grantee.is_empty() {
        "PUBLIC".to_string()
    } else {
        quote_ident(grantee)
    };

    let mut privs = Vec::new();
    for c in privs_str.chars() {
        match c {
            'r' => privs.push("SELECT".to_string()),
            'w' => privs.push("UPDATE".to_string()),
            'a' => privs.push("INSERT".to_string()),
            'd' => privs.push("DELETE".to_string()),
            'D' => privs.push("TRUNCATE".to_string()),
            'x' => privs.push("REFERENCES".to_string()),
            't' => privs.push("TRIGGER".to_string()),
            'U' => privs.push("USAGE".to_string()),
            'C' => privs.push("CREATE".to_string()),
            'c' => privs.push("CONNECT".to_string()),
            'T' => privs.push("TEMPORARY".to_string()),
            'X' => privs.push("EXECUTE".to_string()),
            '*' => {} // grant option — skip
            _ => {}
        }
    }

    if privs.is_empty() {
        return None;
    }

    Some((grantee_name, privs))
}

/// Query ACLs for tables/views and schemas; return GRANT statements.
pub async fn get_privileges(client: &Client, opts: &DumpOptions) -> Result<Vec<PrivilegeInfo>> {
    let mut result = Vec::new();

    // Table / view ACLs.
    let rows = client
        .query(
            "SELECT n.nspname, c.relname, c.relkind::text as relkind,
                    array_to_string(c.relacl, ',') as acl_str
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind IN ('r', 'p', 'v')
               AND c.relacl IS NOT NULL
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
             ORDER BY n.nspname, c.relname",
            &[],
        )
        .await
        .context("query table acls")?;

    for row in &rows {
        let nspname: &str = row.get("nspname");
        let relname: &str = row.get("relname");
        let relkind: &str = row.get("relkind");
        let acl_str: &str = row.get("acl_str");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, nspname) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, nspname) {
            continue;
        }

        let obj_type = if relkind == "v" { "VIEW" } else { "TABLE" };
        let qname = format!("{}.{}", quote_ident(nspname), quote_ident(relname));

        for entry in acl_str.split(',') {
            if entry.is_empty() {
                continue;
            }
            if let Some((grantee, privileges)) = parse_acl_entry(entry) {
                result.push(PrivilegeInfo {
                    statement: format!(
                        "GRANT {} ON {} {} TO {};",
                        privileges.join(", "),
                        obj_type,
                        qname,
                        grantee
                    ),
                });
            }
        }
    }

    // Schema ACLs.
    let schema_rows = client
        .query(
            "SELECT n.nspname, array_to_string(n.nspacl, ',') as acl_str
             FROM pg_catalog.pg_namespace n
             WHERE n.nspacl IS NOT NULL
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
             ORDER BY n.nspname",
            &[],
        )
        .await
        .context("query schema acls")?;

    for row in &schema_rows {
        let nspname: &str = row.get("nspname");
        let acl_str: &str = row.get("acl_str");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, nspname) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, nspname) {
            continue;
        }

        for entry in acl_str.split(',') {
            if entry.is_empty() {
                continue;
            }
            if let Some((grantee, privileges)) = parse_acl_entry(entry) {
                result.push(PrivilegeInfo {
                    statement: format!(
                        "GRANT {} ON SCHEMA {} TO {};",
                        privileges.join(", "),
                        quote_ident(nspname),
                        grantee
                    ),
                });
            }
        }
    }

    Ok(result)
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

/// Metadata for a single comment.
#[derive(Debug, Clone)]
pub struct CommentInfo {
    /// The SQL object type (TABLE, COLUMN, SCHEMA, etc.)
    pub object_type: String,
    /// Fully qualified object name.
    pub object_name: String,
    /// The comment text.
    pub comment: String,
}

/// Query comments from pg_description and pg_shdescription.
pub async fn get_comments(client: &Client, _opts: &DumpOptions) -> Result<Vec<CommentInfo>> {
    let mut comments = Vec::new();

    // Table and column comments from pg_description.
    let rows = client
        .query(
            "SELECT
                CASE
                    WHEN a.attnum IS NULL THEN 'TABLE'
                    ELSE 'COLUMN'
                END AS object_type,
                CASE
                    WHEN a.attnum IS NULL THEN
                        format('%I.%I', n.nspname, c.relname)
                    ELSE
                        format('%I.%I.%I', n.nspname, c.relname, a.attname)
                END AS object_name,
                d.description
             FROM pg_catalog.pg_description d
             JOIN pg_catalog.pg_class c ON c.oid = d.objoid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_attribute a
               ON a.attrelid = d.objoid AND a.attnum = d.objsubid
             WHERE c.relkind IN ('r', 'p', 'v')
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
               AND d.classoid = 'pg_catalog.pg_class'::regclass
             ORDER BY n.nspname, c.relname, d.objsubid",
            &[],
        )
        .await
        .context("query table/column comments")?;

    for row in &rows {
        let object_type: &str = row.get("object_type");
        let object_name: &str = row.get("object_name");
        let comment: &str = row.get("description");
        comments.push(CommentInfo {
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            comment: comment.to_string(),
        });
    }

    // Schema comments from pg_description.
    let schema_rows = client
        .query(
            "SELECT n.nspname, d.description
             FROM pg_catalog.pg_description d
             JOIN pg_catalog.pg_namespace n ON n.oid = d.objoid
             WHERE d.classoid = 'pg_catalog.pg_namespace'::regclass
             ORDER BY n.nspname",
            &[],
        )
        .await
        .context("query schema comments")?;

    for row in &schema_rows {
        let nspname: &str = row.get("nspname");
        let description: &str = row.get("description");
        comments.push(CommentInfo {
            object_type: "SCHEMA".to_string(),
            object_name: quote_ident(nspname),
            comment: description.to_string(),
        });
    }

    // Publication comments from pg_description.
    let pub_rows = client
        .query(
            "SELECT p.pubname, d.description
             FROM pg_catalog.pg_description d
             JOIN pg_catalog.pg_publication p ON p.oid = d.objoid
             WHERE d.classoid = 'pg_catalog.pg_publication'::regclass
             ORDER BY p.pubname",
            &[],
        )
        .await
        .context("query publication comments")?;

    for row in &pub_rows {
        let pubname: &str = row.get("pubname");
        let description: &str = row.get("description");
        comments.push(CommentInfo {
            object_type: "PUBLICATION".to_string(),
            object_name: quote_ident(pubname),
            comment: description.to_string(),
        });
    }

    Ok(comments)
}

/// Metadata for a materialized view.
#[derive(Debug, Clone)]
pub struct MatviewInfo {
    /// Schema name.
    pub schema: String,
    /// Materialized view name.
    pub name: String,
    /// View definition from `pg_get_viewdef`.
    pub definition: String,
    /// Owner role name.
    pub owner: String,
    /// Whether it has been populated (can be refreshed).
    pub is_populated: bool,
}

impl MatviewInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a function or procedure.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// Schema name.
    pub schema: String,
    /// Function/procedure name.
    pub name: String,
    /// Full DDL from pg_get_functiondef.
    pub definition: String,
    /// 'f' = function, 'p' = procedure, 'a' = aggregate, 'w' = window.
    pub prokind: char,
}

impl FunctionInfo {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a trigger.
#[derive(Debug, Clone)]
pub struct TriggerInfo {
    /// Schema of the table the trigger is on.
    pub schema: String,
    /// Table name the trigger is on.
    pub table_name: String,
    /// Trigger name.
    pub name: String,
    /// Full trigger DDL from pg_get_triggerdef.
    pub definition: String,
    /// tgenabled: 'O'=enabled, 'D'=disabled, 'R'=replica, 'A'=always.
    pub enabled: char,
    /// Whether this is an internal (constraint) trigger.
    pub is_internal: bool,
}

/// Metadata for an event trigger.
#[derive(Debug, Clone)]
pub struct EventTriggerInfo {
    /// Event trigger name.
    pub name: String,
    /// Event (e.g. 'ddl_command_start').
    pub event: String,
    /// Function name.
    pub func_name: String,
    /// Function schema.
    pub func_schema: String,
    /// enabled: 'O'=enabled, 'D'=disabled, 'R'=replica, 'A'=always.
    pub enabled: char,
    /// Tag filter (comma-separated), or empty.
    pub tags: String,
}

/// Metadata for extended statistics.
#[derive(Debug, Clone)]
pub struct ExtendedStatInfo {
    /// Schema name.
    pub schema: String,
    /// Statistics object name.
    pub name: String,
    /// DDL from pg_get_statisticsobjdef.
    pub definition: String,
    /// Statistics target (-1 = default; None means unset on PG17+).
    /// Stored as i32 to handle both PG 16 (integer) and PG 17 (smallint/NULL).
    pub stattarget: Option<i32>,
}

/// Metadata for a CREATE TRANSFORM.
#[derive(Debug, Clone)]
pub struct TransformInfo {
    /// Type name (e.g. `integer`).
    pub type_name: String,
    /// Language name (e.g. `plpythonu`).
    pub lang_name: String,
    /// FROM SQL function name (or empty).
    pub fromsql: String,
    /// TO SQL function name (or empty).
    pub tosql: String,
}

/// Metadata for a type comment.
#[derive(Debug, Clone)]
pub struct TypeCommentInfo {
    /// Type name (qualified).
    pub type_name: String,
    /// Comment text.
    pub comment: String,
}

/// Metadata for a foreign data wrapper.
#[derive(Debug, Clone)]
pub struct FdwInfo {
    /// FDW name.
    pub name: String,
    /// Owner role name.
    pub owner: String,
    /// Handler function name (or empty).
    pub handler: String,
    /// Validator function name (or empty).
    pub validator: String,
    /// Formatted FDW options (or empty).
    pub options: String,
}

/// Metadata for a foreign server.
#[derive(Debug, Clone)]
pub struct ForeignServerInfo {
    /// Server name.
    pub name: String,
    /// Owner role name.
    pub owner: String,
    /// FDW name.
    pub fdw_name: String,
    /// Server type (or empty).
    pub server_type: String,
    /// Server version (or empty).
    pub server_version: String,
    /// Formatted server options (or empty).
    pub options: String,
}

/// Metadata for a foreign table.
#[derive(Debug, Clone)]
pub struct ForeignTableInfo {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Owner role name.
    pub owner: String,
    /// Foreign server name.
    pub server_name: String,
    /// Formatted table-level options (or empty).
    pub options: String,
    /// Columns with their types and FDW options.
    pub columns: Vec<ForeignColumnInfo>,
}

impl ForeignTableInfo {
    /// Fully qualified name: `schema.name`.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// Metadata for a foreign table column.
#[derive(Debug, Clone)]
pub struct ForeignColumnInfo {
    /// Column name.
    pub name: String,
    /// Full type name.
    pub type_name: String,
    /// Whether the column has a NOT NULL constraint.
    pub not_null: bool,
    /// Default expression, if any.
    pub default_expr: Option<String>,
    /// Raw column-level FDW options (`key=value, ...` from catalog).
    pub options_raw: String,
}

/// Metadata for a user mapping.
#[derive(Debug, Clone)]
pub struct UserMappingInfo {
    /// User name (or `PUBLIC`).
    pub username: String,
    /// Server name.
    pub server_name: String,
    /// Formatted options (or empty).
    pub options: String,
}

/// Metadata for a publication.
#[derive(Debug, Clone)]
pub struct PublicationInfo {
    /// Publication name.
    pub name: String,
    /// Owner role name.
    pub owner: String,
    /// Whether it publishes all tables.
    pub all_tables: bool,
    /// Publish INSERT.
    pub pub_insert: bool,
    /// Publish UPDATE.
    pub pub_update: bool,
    /// Publish DELETE.
    pub pub_delete: bool,
    /// Publish TRUNCATE.
    pub pub_truncate: bool,
    /// Tables in the publication.
    pub tables: Vec<PublicationTableInfo>,
    /// Schemas in the publication.
    pub schemas: Vec<String>,
}

/// A table in a publication.
#[derive(Debug, Clone)]
pub struct PublicationTableInfo {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// WHERE filter expression (or empty).
    pub where_clause: String,
}

/// Format PostgreSQL FDW options from catalog representation to SQL.
///
/// Options are stored as `key=value` entries separated by `, `.
/// This formats them as SQL: `key 'value', key2 'value2'`.
pub fn format_fdw_options(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    raw.split(", ")
        .filter_map(|kv| {
            let eq = kv.find('=')?;
            let key = &kv[..eq];
            let val = &kv[eq + 1..];
            Some(format!("{} '{}'", key, val.replace('\'', "''")))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Query user-defined materialized views from the catalog.
pub async fn get_matviews(client: &Client, opts: &DumpOptions) -> Result<Vec<MatviewInfo>> {
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
                    r.rolname AS owner,
                    c.relispopulated AS is_populated,
                    pg_catalog.pg_get_viewdef(c.oid, true) AS definition
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
             WHERE c.relkind = 'm'
               AND n.nspname != all($1)
             ORDER BY n.nspname, c.relname",
            &[&excluded],
        )
        .await
        .context("query materialized views")?;

    let mut matviews = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let definition: Option<String> = row.get("definition");
        matviews.push(MatviewInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("view_name").to_string(),
            owner: row.get::<_, &str>("owner").to_string(),
            is_populated: row.get("is_populated"),
            definition: definition.unwrap_or_default(),
        });
    }

    Ok(matviews)
}

/// Query user-defined functions and procedures from the catalog.
///
/// Excludes aggregates, window functions, and functions in system schemas.
/// Only returns regular functions ('f') and procedures ('p').
pub async fn get_functions(client: &Client, opts: &DumpOptions) -> Result<Vec<FunctionInfo>> {
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
                    p.proname AS func_name,
                    p.prokind::text AS prokind,
                    pg_catalog.pg_get_functiondef(p.oid) AS definition
             FROM pg_catalog.pg_proc p
             JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
             WHERE p.prokind IN ('f', 'p')
               AND n.nspname != all($1)
             ORDER BY n.nspname, p.proname",
            &[&excluded],
        )
        .await
        .context("query functions")?;

    let mut functions = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let prokind_str: &str = row.get("prokind");
        let prokind = prokind_str.chars().next().unwrap_or('f');
        let definition: Option<String> = row.get("definition");

        functions.push(FunctionInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("func_name").to_string(),
            definition: definition.unwrap_or_default(),
            prokind,
        });
    }

    Ok(functions)
}

/// Query user-defined triggers (non-internal) from the catalog.
pub async fn get_triggers(client: &Client, opts: &DumpOptions) -> Result<Vec<TriggerInfo>> {
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
                    c.relname AS table_name,
                    t.tgname AS trigger_name,
                    t.tgenabled::text AS enabled,
                    t.tgisinternal AS is_internal,
                    pg_catalog.pg_get_triggerdef(t.oid, true) AS definition
             FROM pg_catalog.pg_trigger t
             JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE NOT t.tgisinternal
               AND n.nspname != all($1)
             ORDER BY n.nspname, c.relname, t.tgname",
            &[&excluded],
        )
        .await
        .context("query triggers")?;

    let mut triggers = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let enabled_str: &str = row.get("enabled");
        let enabled = enabled_str.chars().next().unwrap_or('O');

        triggers.push(TriggerInfo {
            schema: schema.to_string(),
            table_name: row.get::<_, &str>("table_name").to_string(),
            name: row.get::<_, &str>("trigger_name").to_string(),
            definition: row.get::<_, &str>("definition").to_string(),
            enabled,
            is_internal: row.get("is_internal"),
        });
    }

    Ok(triggers)
}

/// Query event triggers from the catalog.
pub async fn get_event_triggers(client: &Client) -> Result<Vec<EventTriggerInfo>> {
    let rows = client
        .query(
            "SELECT et.evtname AS name,
                    et.evtevent AS event,
                    et.evtenabled::text AS enabled,
                    n.nspname AS func_schema,
                    p.proname AS func_name,
                    COALESCE(array_to_string(et.evttags, ', '), '') AS tags
             FROM pg_catalog.pg_event_trigger et
             JOIN pg_catalog.pg_proc p ON p.oid = et.evtfoid
             JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
             ORDER BY et.evtname",
            &[],
        )
        .await
        .context("query event triggers")?;

    let mut event_triggers = Vec::new();
    for row in &rows {
        let enabled_str: &str = row.get("enabled");
        let enabled = enabled_str.chars().next().unwrap_or('O');

        event_triggers.push(EventTriggerInfo {
            name: row.get::<_, &str>("name").to_string(),
            event: row.get::<_, &str>("event").to_string(),
            func_schema: row.get::<_, &str>("func_schema").to_string(),
            func_name: row.get::<_, &str>("func_name").to_string(),
            enabled,
            tags: row.get::<_, &str>("tags").to_string(),
        });
    }

    Ok(event_triggers)
}

/// Query extended statistics objects from the catalog.
pub async fn get_extended_statistics(
    client: &Client,
    opts: &DumpOptions,
) -> Result<Vec<ExtendedStatInfo>> {
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
                    s.stxname AS stat_name,
                    pg_catalog.pg_get_statisticsobjdef(s.oid) AS definition,
                    s.stxstattarget::bigint AS stattarget
             FROM pg_catalog.pg_statistic_ext s
             JOIN pg_catalog.pg_namespace n ON n.oid = s.stxnamespace
             WHERE n.nspname != all($1)
             ORDER BY n.nspname, s.stxname",
            &[&excluded],
        )
        .await
        .context("query extended statistics")?;

    let mut stats = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let definition: Option<String> = row.get("definition");
        stats.push(ExtendedStatInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("stat_name").to_string(),
            definition: definition.unwrap_or_default(),
            stattarget: row.get::<_, Option<i64>>("stattarget").map(|v| v as i32),
        });
    }

    Ok(stats)
}

/// Query transforms from the catalog.
pub async fn get_transforms(client: &Client) -> Result<Vec<TransformInfo>> {
    let rows = client
        .query(
            "SELECT pg_catalog.format_type(t.trftype, NULL) AS type_name,
                    l.lanname AS lang_name,
                    COALESCE((SELECT n.nspname || '.' || p.proname
                              FROM pg_catalog.pg_proc p
                              JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                              WHERE p.oid = t.trffromsql), '') AS fromsql,
                    COALESCE((SELECT n.nspname || '.' || p.proname
                              FROM pg_catalog.pg_proc p
                              JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                              WHERE p.oid = t.trftosql), '') AS tosql
             FROM pg_catalog.pg_transform t
             JOIN pg_catalog.pg_language l ON l.oid = t.trflang
             ORDER BY type_name, lang_name",
            &[],
        )
        .await
        .context("query transforms")?;

    let mut transforms = Vec::new();
    for row in &rows {
        transforms.push(TransformInfo {
            type_name: row.get::<_, &str>("type_name").to_string(),
            lang_name: row.get::<_, &str>("lang_name").to_string(),
            fromsql: row.get::<_, &str>("fromsql").to_string(),
            tosql: row.get::<_, &str>("tosql").to_string(),
        });
    }

    Ok(transforms)
}

/// Query comments on type objects (ENUM, RANGE, composite, base types, domains).
pub async fn get_type_comments(
    client: &Client,
    opts: &DumpOptions,
) -> Result<Vec<TypeCommentInfo>> {
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
            "SELECT format('%I.%I', n.nspname, t.typname) AS type_name,
                    n.nspname,
                    d.description
             FROM pg_catalog.pg_type t
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
             JOIN pg_catalog.pg_description d ON d.objoid = t.oid
               AND d.classoid = 'pg_catalog.pg_type'::regclass
             WHERE n.nspname != all($1)
               AND t.typtype IN ('e', 'r', 'c', 'b', 'd', 'p')
             ORDER BY n.nspname, t.typname",
            &[&excluded],
        )
        .await
        .context("query type comments")?;

    let mut comments = Vec::new();
    for row in &rows {
        let schema: &str = row.get("nspname");
        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }
        comments.push(TypeCommentInfo {
            type_name: row.get::<_, &str>("type_name").to_string(),
            comment: row.get::<_, &str>("description").to_string(),
        });
    }

    Ok(comments)
}

/// Query foreign data wrappers from the catalog.
pub async fn get_fdws(client: &Client) -> Result<Vec<FdwInfo>> {
    let rows = client
        .query(
            "SELECT fdw.fdwname,
                    r.rolname AS owner,
                    COALESCE(h.proname, '') AS handler,
                    COALESCE(v.proname, '') AS validator,
                    COALESCE(array_to_string(fdw.fdwoptions, ', '), '') AS options
             FROM pg_catalog.pg_foreign_data_wrapper fdw
             JOIN pg_catalog.pg_roles r ON r.oid = fdw.fdwowner
             LEFT JOIN pg_catalog.pg_proc h ON h.oid = fdw.fdwhandler
             LEFT JOIN pg_catalog.pg_proc v ON v.oid = fdw.fdwvalidator
             ORDER BY fdw.fdwname",
            &[],
        )
        .await
        .context("query foreign data wrappers")?;

    let mut fdws = Vec::new();
    for row in &rows {
        fdws.push(FdwInfo {
            name: row.get::<_, &str>("fdwname").to_string(),
            owner: row.get::<_, &str>("owner").to_string(),
            handler: row.get::<_, &str>("handler").to_string(),
            validator: row.get::<_, &str>("validator").to_string(),
            options: row.get::<_, &str>("options").to_string(),
        });
    }

    Ok(fdws)
}

/// Query foreign servers from the catalog.
pub async fn get_foreign_servers(client: &Client) -> Result<Vec<ForeignServerInfo>> {
    let rows = client
        .query(
            "SELECT s.srvname,
                    r.rolname AS owner,
                    fdw.fdwname,
                    COALESCE(s.srvtype, '') AS srvtype,
                    COALESCE(s.srvversion, '') AS srvversion,
                    COALESCE(array_to_string(s.srvoptions, ', '), '') AS options
             FROM pg_catalog.pg_foreign_server s
             JOIN pg_catalog.pg_roles r ON r.oid = s.srvowner
             JOIN pg_catalog.pg_foreign_data_wrapper fdw ON fdw.oid = s.srvfdw
             ORDER BY s.srvname",
            &[],
        )
        .await
        .context("query foreign servers")?;

    let mut servers = Vec::new();
    for row in &rows {
        servers.push(ForeignServerInfo {
            name: row.get::<_, &str>("srvname").to_string(),
            owner: row.get::<_, &str>("owner").to_string(),
            fdw_name: row.get::<_, &str>("fdwname").to_string(),
            server_type: row.get::<_, &str>("srvtype").to_string(),
            server_version: row.get::<_, &str>("srvversion").to_string(),
            options: row.get::<_, &str>("options").to_string(),
        });
    }

    Ok(servers)
}

/// Query foreign tables from the catalog.
pub async fn get_foreign_tables(
    client: &Client,
    opts: &DumpOptions,
) -> Result<Vec<ForeignTableInfo>> {
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
                    c.relname AS table_name,
                    r.rolname AS owner,
                    s.srvname AS server_name,
                    COALESCE(array_to_string(ft.ftoptions, ', '), '') AS options,
                    c.oid::bigint AS table_oid
             FROM pg_catalog.pg_foreign_table ft
             JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
             JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver
             WHERE n.nspname != all($1)
             ORDER BY n.nspname, c.relname",
            &[&excluded],
        )
        .await
        .context("query foreign tables")?;

    let mut tables = Vec::new();
    for row in &rows {
        let schema: &str = row.get("schema_name");

        if !opts.schemas.is_empty() && !super::filter::schema_matches_any(&opts.schemas, schema) {
            continue;
        }
        if super::filter::schema_matches_any(&opts.exclude_schemas, schema) {
            continue;
        }

        let table_oid = row.get::<_, i64>("table_oid") as u32;
        let columns = get_foreign_columns(client, table_oid).await?;

        tables.push(ForeignTableInfo {
            schema: schema.to_string(),
            name: row.get::<_, &str>("table_name").to_string(),
            owner: row.get::<_, &str>("owner").to_string(),
            server_name: row.get::<_, &str>("server_name").to_string(),
            options: row.get::<_, &str>("options").to_string(),
            columns,
        });
    }

    Ok(tables)
}

/// Query columns for a foreign table by OID, including FDW column options.
async fn get_foreign_columns(client: &Client, table_oid: u32) -> Result<Vec<ForeignColumnInfo>> {
    let oid_i64 = table_oid as i64;
    let rows = client
        .query(
            "SELECT a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
                    a.attnotnull,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr,
                    COALESCE(array_to_string(a.attfdwoptions, ', '), '') AS options
             FROM pg_catalog.pg_attribute a
             LEFT JOIN pg_catalog.pg_attrdef d
               ON d.adrelid = a.attrelid AND d.adnum = a.attnum
             WHERE a.attrelid = $1::bigint::oid
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&oid_i64],
        )
        .await
        .context("query foreign columns")?;

    let mut columns = Vec::new();
    for row in &rows {
        columns.push(ForeignColumnInfo {
            name: row.get("attname"),
            type_name: row.get("type_name"),
            not_null: row.get("attnotnull"),
            default_expr: row.get("default_expr"),
            options_raw: row.get::<_, &str>("options").to_string(),
        });
    }
    Ok(columns)
}

/// Query user mappings from the catalog.
pub async fn get_user_mappings(client: &Client) -> Result<Vec<UserMappingInfo>> {
    let rows = client
        .query(
            "SELECT COALESCE(r.rolname, 'PUBLIC') AS username,
                    s.srvname AS server_name,
                    COALESCE(array_to_string(um.umoptions, ', '), '') AS options
             FROM pg_catalog.pg_user_mapping um
             JOIN pg_catalog.pg_foreign_server s ON s.oid = um.umserver
             LEFT JOIN pg_catalog.pg_roles r ON r.oid = um.umuser
             ORDER BY s.srvname, username",
            &[],
        )
        .await
        .context("query user mappings")?;

    let mut mappings = Vec::new();
    for row in &rows {
        mappings.push(UserMappingInfo {
            username: row.get::<_, &str>("username").to_string(),
            server_name: row.get::<_, &str>("server_name").to_string(),
            options: row.get::<_, &str>("options").to_string(),
        });
    }

    Ok(mappings)
}

/// Query publications from the catalog, including their table and schema memberships.
pub async fn get_publications(client: &Client) -> Result<Vec<PublicationInfo>> {
    let rows = client
        .query(
            "SELECT p.oid::bigint AS pub_oid,
                    p.pubname,
                    r.rolname AS owner,
                    p.puballtables,
                    p.pubinsert,
                    p.pubupdate,
                    p.pubdelete,
                    p.pubtruncate
             FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_roles r ON r.oid = p.pubowner
             ORDER BY p.pubname",
            &[],
        )
        .await
        .context("query publications")?;

    let mut publications = Vec::new();
    for row in &rows {
        let pub_oid: i64 = row.get("pub_oid");

        // Query tables for this publication.
        let table_rows = client
            .query(
                "SELECT n.nspname AS schema_name,
                        c.relname AS table_name,
                        COALESCE(pg_catalog.pg_get_expr(pr.prqual, pr.prrelid), '') AS where_clause
                 FROM pg_catalog.pg_publication_rel pr
                 JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE pr.prpubid = $1::bigint::oid
                 ORDER BY n.nspname, c.relname",
                &[&pub_oid],
            )
            .await
            .context("query publication tables")?;

        let mut pub_tables = Vec::new();
        for tr in &table_rows {
            pub_tables.push(PublicationTableInfo {
                schema: tr.get::<_, &str>("schema_name").to_string(),
                name: tr.get::<_, &str>("table_name").to_string(),
                where_clause: tr.get::<_, &str>("where_clause").to_string(),
            });
        }

        // Query schemas for this publication.
        let schema_rows = client
            .query(
                "SELECT n.nspname AS schema_name
                 FROM pg_catalog.pg_publication_namespace pn
                 JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
                 WHERE pn.pnpubid = $1::bigint::oid
                 ORDER BY n.nspname",
                &[&pub_oid],
            )
            .await
            .context("query publication schemas")?;

        let pub_schemas: Vec<String> = schema_rows
            .iter()
            .map(|r| r.get::<_, &str>("schema_name").to_string())
            .collect();

        publications.push(PublicationInfo {
            name: row.get::<_, &str>("pubname").to_string(),
            owner: row.get::<_, &str>("owner").to_string(),
            all_tables: row.get("puballtables"),
            pub_insert: row.get("pubinsert"),
            pub_update: row.get("pubupdate"),
            pub_delete: row.get("pubdelete"),
            pub_truncate: row.get("pubtruncate"),
            tables: pub_tables,
            schemas: pub_schemas,
        });
    }

    Ok(publications)
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
