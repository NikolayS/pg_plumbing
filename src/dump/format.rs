// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Output formats for pg_dump: plain, custom, directory, tar.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::catalog::{
    format_fdw_options, quote_ident, ConstraintInfo, EventTriggerInfo, ExtendedStatInfo, FdwInfo,
    ForeignServerInfo, ForeignTableInfo, FunctionInfo, MatviewInfo, PrivilegeInfo, PublicationInfo,
    SchemaInfo, SequenceInfo, TableInfo, TransformInfo, TriggerInfo, TypeCommentInfo,
    UserMappingInfo, ViewInfo,
};
use super::DumpOptions;

/// Write a `CREATE TABLE` statement to the output buffer, followed by any
/// `ALTER TABLE … ADD CONSTRAINT` statements for non-inline constraints.
///
/// Handles three cases:
/// - Regular table: standard column-list CREATE TABLE.
/// - Partitioned table: CREATE TABLE ... PARTITION BY <key>.
/// - Partition child: CREATE TABLE <child> PARTITION OF <parent> <bound>.
///
/// Constraint emission rules (matching real pg_dump behaviour):
/// - CHECK constraints: emitted **inline** inside the CREATE TABLE column list.
/// - PRIMARY KEY, UNIQUE, FOREIGN KEY, NOT NULL: emitted as
///   `ALTER TABLE [ONLY] <table> ADD CONSTRAINT <name> <def>;` after the
///   CREATE TABLE statement.  ONLY is omitted for partitioned tables.
pub fn write_create_table(out: &mut String, table: &TableInfo) {
    let qname = table.qualified_name();

    out.push_str(&format!("--\n-- Name: {}; Type: TABLE\n--\n\n", table.name));

    // Partition child: `CREATE TABLE child PARTITION OF parent <bound>;`
    if let (Some(ref bound), Some(ref parent)) = (&table.partition_bound, &table.parent_table) {
        // Use the parent's own schema (may differ from the child's schema).
        let parent_schema = table
            .parent_schema
            .as_deref()
            .unwrap_or(table.schema.as_str());
        let parent_qname = format!("{}.{}", quote_ident(parent_schema), quote_ident(parent));
        out.push_str(&format!(
            "CREATE TABLE {qname} PARTITION OF {parent_qname} {bound};\n"
        ));
        // Partition children inherit constraints from parent; no ALTER TABLE needed here.
        return;
    }

    // Separate inline constraints (CHECK) from post-create ones (PK, UNIQUE, FK, NOT NULL).
    let inline_checks: Vec<&ConstraintInfo> = table
        .constraints
        .iter()
        .filter(|c| c.contype == 'c')
        .collect();

    let post_constraints: Vec<&ConstraintInfo> = table
        .constraints
        .iter()
        .filter(|c| c.contype != 'c')
        .collect();

    // Partitioned parent or regular table — write column list.
    out.push_str(&format!("CREATE TABLE {qname} (\n"));

    let total_items = table.columns.len() + inline_checks.len();
    for (i, col) in table.columns.iter().enumerate() {
        out.push_str(&format!("    {} {}", quote_ident(&col.name), col.type_name));
        if col.not_null {
            out.push_str(" NOT NULL");
        }
        if let Some(ref default) = col.default_expr {
            out.push_str(&format!(" DEFAULT {default}"));
        }
        // Add trailing comma if not the last item (columns or inline CHECK constraints follow).
        if i + 1 < total_items {
            out.push(',');
        }
        out.push('\n');
    }

    // Emit inline CHECK constraints.
    for (i, chk) in inline_checks.iter().enumerate() {
        out.push_str(&format!(
            "    CONSTRAINT {} {}",
            quote_ident(&chk.name),
            chk.definition
        ));
        if i + 1 < inline_checks.len() {
            out.push(',');
        }
        out.push('\n');
    }

    out.push(')');

    // Append PARTITION BY clause for partitioned tables.
    if let Some(ref partkey) = table.partition_key {
        out.push_str(&format!("\nPARTITION BY {partkey}"));
    }

    out.push_str(";\n");

    // Emit post-create constraints as ALTER TABLE … ADD CONSTRAINT.
    // Use ONLY for regular tables; omit ONLY for partitioned tables
    // (matching real pg_dump behaviour).
    let only_kw = if table.partition_key.is_some() {
        ""
    } else {
        "ONLY "
    };

    for con in &post_constraints {
        let con_type_label = match con.contype {
            'f' => "FK CONSTRAINT",
            'p' => "CONSTRAINT",
            'u' => "CONSTRAINT",
            'n' => "CONSTRAINT",
            _ => "CONSTRAINT",
        };
        out.push_str(&format!(
            "\n--\n-- Name: {} {}; Type: {}\n--\n\nALTER TABLE {only_kw}{qname}\n    ADD CONSTRAINT {} {};\n",
            table.name,
            con.name,
            con_type_label,
            quote_ident(&con.name),
            con.definition
        ));
    }
}

/// Write a `CREATE SEQUENCE` statement to the output buffer.
pub fn write_create_sequence(out: &mut String, seq: &SequenceInfo) {
    let qname = seq.qualified_name();
    out.push_str(&format!(
        "--\n-- Name: {}; Type: SEQUENCE\n--\n\n",
        seq.name
    ));
    out.push_str(&format!("CREATE SEQUENCE {qname}\n"));
    out.push_str(&format!("    START WITH {}\n", seq.start_value));
    out.push_str(&format!("    INCREMENT BY {}\n", seq.increment_by));
    out.push_str(&format!("    MINVALUE {}\n", seq.min_value));
    out.push_str(&format!("    MAXVALUE {}\n", seq.max_value));
    if seq.cycle {
        out.push_str("    CYCLE\n");
    } else {
        out.push_str("    NO CYCLE\n");
    }
    out.push_str(&format!("    CACHE {};\n", seq.cache_size));
}

/// Write `ALTER SEQUENCE … OWNED BY` statement if the sequence has an owner.
pub fn write_alter_sequence(out: &mut String, seq: &SequenceInfo) {
    if let (Some(ref owned_schema), Some(ref owned_table), Some(ref owned_col)) = (
        &seq.owned_by_schema,
        &seq.owned_by_table,
        &seq.owned_by_column,
    ) {
        let qname = seq.qualified_name();
        let owned_col_q = quote_ident(owned_col);
        out.push_str(&format!(
            "\n--\n-- Name: {}; Type: SEQUENCE OWNED BY\n--\n\nALTER SEQUENCE {qname} OWNED BY {}.{}.{owned_col_q};\n",
            seq.name,
            quote_ident(owned_schema),
            quote_ident(owned_table),
        ));
    }
}

/// Write a `CREATE OR REPLACE VIEW` statement to the output buffer.
pub fn write_create_view(out: &mut String, view: &ViewInfo) {
    let qname = view.qualified_name();
    out.push_str(&format!("--\n-- Name: {}; Type: VIEW\n--\n\n", view.name));
    out.push_str(&format!("CREATE OR REPLACE VIEW {qname} AS\n"));
    let def = view.definition.trim_end();
    out.push_str(def);
    if !def.ends_with(';') {
        out.push(';');
    }
    out.push('\n');
}

/// Write an `ALTER TABLE [ONLY] … OWNER TO …;` statement.
///
/// Uses `ONLY` for regular tables; omits it for partitioned tables and
/// partition children (matching real pg_dump behaviour).
pub fn write_alter_table_owner(out: &mut String, table: &TableInfo) {
    let qname = table.qualified_name();
    let only = if table.partition_key.is_none() && table.partition_bound.is_none() {
        "ONLY "
    } else {
        ""
    };
    out.push_str(&format!(
        "ALTER TABLE {only}{qname} OWNER TO {};\n",
        quote_ident(&table.owner)
    ));
}

/// Write a `CREATE SCHEMA` statement.
pub fn write_create_schema(out: &mut String, schema: &SchemaInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: SCHEMA\n--\n\nCREATE SCHEMA {};\n",
        schema.name,
        quote_ident(&schema.name),
    ));
}

/// Write a `DROP SCHEMA [IF EXISTS]` statement.
pub fn write_drop_schema(out: &mut String, schema: &SchemaInfo, if_exists: bool) {
    if if_exists {
        out.push_str(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE;\n",
            quote_ident(&schema.name),
        ));
    } else {
        out.push_str(&format!(
            "DROP SCHEMA {} CASCADE;\n",
            quote_ident(&schema.name),
        ));
    }
}

/// Write an `ALTER SCHEMA … OWNER TO …;` statement.
pub fn write_alter_schema_owner(out: &mut String, schema: &SchemaInfo) {
    out.push_str(&format!(
        "ALTER SCHEMA {} OWNER TO {};\n",
        quote_ident(&schema.name),
        quote_ident(&schema.owner)
    ));
}

/// Write `COMMENT ON …` statements.
pub fn write_comments(out: &mut String, comments: &[super::catalog::CommentInfo]) {
    for c in comments {
        let escaped = c.comment.replace('\'', "''");
        out.push_str(&format!(
            "COMMENT ON {} {} IS '{}';\n",
            c.object_type, c.object_name, escaped
        ));
    }
}

/// Write GRANT privilege statements to the output buffer.
pub fn write_privileges(out: &mut String, privs: &[PrivilegeInfo]) {
    for p in privs {
        out.push_str(&p.statement);
        out.push('\n');
    }
}

/// Write table data as a raw COPY data string (rows only, no header/footer).
///
/// Returns just the tab-separated rows suitable for embedding in a custom archive.
pub async fn write_table_data_to_string(
    client: &Client,
    table: &TableInfo,
    opts: &DumpOptions,
) -> Result<String> {
    let qname = table.qualified_name();
    // Cast each column to text to handle custom types (enums, domains, etc.)
    // that tokio-postgres cannot decode by OID at runtime.
    let col_list: String = table
        .columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!("select {col_list} from {qname}");
    let rows = client
        .query(&query, &[])
        .await
        .with_context(|| format!("query data from {qname}"))?;

    if rows.is_empty() {
        return Ok(String::new());
    }

    let mut data = String::new();
    for row in &rows {
        let mut values = Vec::new();
        for (i, _col) in table.columns.iter().enumerate() {
            values.push(format_copy_value(row, i));
        }
        data.push_str(&values.join("\t"));
        data.push('\n');
    }

    // For custom format, we always write COPY data regardless of --inserts.
    // inserts flag only applies to plain format.
    let _ = opts;

    Ok(data)
}

/// Write table data as COPY or INSERT statements.
pub async fn write_table_data(
    out: &mut String,
    client: &Client,
    table: &TableInfo,
    opts: &DumpOptions,
) -> Result<()> {
    let qname = table.qualified_name();
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();
    // Cast each column to text to handle custom types (enums, domains, etc.)
    let col_list_cast: String = col_names
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!("select {col_list_cast} from {qname}");
    let rows = client
        .query(&query, &[])
        .await
        .with_context(|| format!("query data from {qname}"))?;

    if rows.is_empty() {
        return Ok(());
    }

    if opts.inserts {
        write_inserts(out, table, &rows, opts)?;
    } else {
        write_copy(out, table, &rows)?;
    }

    Ok(())
}

/// Write data as COPY ... FROM stdin.
fn write_copy(out: &mut String, table: &TableInfo, rows: &[tokio_postgres::Row]) -> Result<()> {
    // For partition children, COPY into the parent table so that the
    // partitioning logic routes each row to the correct child during restore.
    // This is required for non-integer partition keys (e.g. enums) whose hash
    // depends on catalog OIDs that differ across databases.
    let copy_target_qname = if let (Some(ref parent), Some(ref parent_schema)) =
        (&table.parent_table, &table.parent_schema)
    {
        format!("{}.{}", quote_ident(parent_schema), quote_ident(parent))
    } else if let Some(ref parent) = &table.parent_table {
        format!("{}.{}", quote_ident(&table.schema), quote_ident(parent))
    } else {
        table.qualified_name()
    };
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let col_list = col_names.join(", ");

    out.push_str(&format!(
        "--\n-- Data for Name: {}; Type: TABLE DATA\n--\n\n",
        table.name
    ));
    out.push_str(&format!(
        "COPY {copy_target_qname} ({col_list}) FROM stdin;\n"
    ));

    for row in rows {
        let mut values = Vec::new();
        for (i, _col) in table.columns.iter().enumerate() {
            let val = format_copy_value(row, i);
            values.push(val);
        }
        out.push_str(&values.join("\t"));
        out.push('\n');
    }

    out.push_str("\\.\n");
    Ok(())
}

/// Write data as INSERT statements.
fn write_inserts(
    out: &mut String,
    table: &TableInfo,
    rows: &[tokio_postgres::Row],
    opts: &DumpOptions,
) -> Result<()> {
    // For partition children, INSERT into the parent table for the same reason
    // as write_copy: enum-hashed partitions need re-routing during restore.
    let insert_target_qname = if let (Some(ref parent), Some(ref parent_schema)) =
        (&table.parent_table, &table.parent_schema)
    {
        format!("{}.{}", quote_ident(parent_schema), quote_ident(parent))
    } else if let Some(ref parent) = &table.parent_table {
        format!("{}.{}", quote_ident(&table.schema), quote_ident(parent))
    } else {
        table.qualified_name()
    };
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();

    out.push_str(&format!(
        "--\n-- Data for Name: {}; Type: TABLE DATA\n--\n\n",
        table.name
    ));

    let prefix = if opts.column_inserts {
        format!(
            "INSERT INTO {insert_target_qname} ({}) VALUES",
            col_names.join(", ")
        )
    } else {
        format!("INSERT INTO {insert_target_qname} VALUES")
    };

    let rows_per = opts.rows_per_insert.unwrap_or(1) as usize;

    for chunk in rows.chunks(rows_per) {
        out.push_str(&prefix);
        for (ci, row) in chunk.iter().enumerate() {
            if ci > 0 {
                out.push(',');
            }
            out.push_str(" (");
            for (i, _col) in table.columns.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let val = format_insert_value(row, i);
                out.push_str(&val);
            }
            out.push(')');
        }
        out.push_str(";\n");
    }

    Ok(())
}

/// Format a single value for COPY output (tab-separated, `\N` for NULL).
fn format_copy_value(row: &tokio_postgres::Row, idx: usize) -> String {
    // Try common types; fall back to text representation.
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        match v {
            Some(s) => escape_copy_value(&s),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        match v {
            Some(b) => if b { "t" } else { "f" }.to_string(),
            None => "\\N".to_string(),
        }
    } else {
        "\\N".to_string()
    }
}

/// Escape special characters in a COPY value.
fn escape_copy_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Format a single value for INSERT output (SQL literals).
fn format_insert_value(row: &tokio_postgres::Row, idx: usize) -> String {
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        match v {
            Some(s) => format!("'{}'", s.replace('\'', "''")),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        match v {
            Some(b) => if b { "true" } else { "false" }.to_string(),
            None => "NULL".to_string(),
        }
    } else {
        "NULL".to_string()
    }
}

/// Write a `CREATE MATERIALIZED VIEW` statement + `REFRESH MATERIALIZED VIEW`.
pub fn write_create_matview(out: &mut String, mv: &MatviewInfo) {
    let qname = mv.qualified_name();
    out.push_str(&format!(
        "--\n-- Name: {}; Type: MATERIALIZED VIEW\n--\n\n",
        mv.name
    ));
    out.push_str(&format!("CREATE MATERIALIZED VIEW {qname} AS\n"));
    let def = mv.definition.trim_end();
    out.push_str(def);
    if !def.ends_with(';') {
        out.push(';');
    }
    out.push('\n');
}

/// Write `ALTER MATERIALIZED VIEW … OWNER TO …`.
pub fn write_alter_matview_owner(out: &mut String, mv: &MatviewInfo) {
    let qname = mv.qualified_name();
    out.push_str(&format!(
        "ALTER MATERIALIZED VIEW {qname} OWNER TO {};\n",
        quote_ident(&mv.owner)
    ));
}

/// Write `REFRESH MATERIALIZED VIEW` for populated matviews.
pub fn write_refresh_matview(out: &mut String, mv: &MatviewInfo) {
    if mv.is_populated {
        let qname = mv.qualified_name();
        out.push_str(&format!("REFRESH MATERIALIZED VIEW {qname};\n"));
    }
}

/// Write a function or procedure DDL (from pg_get_functiondef).
pub fn write_create_function(out: &mut String, func: &FunctionInfo) {
    let kind = if func.prokind == 'p' {
        "PROCEDURE"
    } else {
        "FUNCTION"
    };
    out.push_str(&format!(
        "--\n-- Name: {}; Type: {}\n--\n\n",
        func.name, kind
    ));
    let def = func.definition.trim_end();
    out.push_str(def);
    if !def.ends_with(';') {
        out.push(';');
    }
    out.push('\n');
}

/// Write a trigger DDL.
///
/// Note: DISABLE TRIGGER is NOT emitted here — it is handled separately
/// by `write_disable_trigger_all` in the mod.rs pipeline, after all triggers
/// for a table have been emitted.
pub fn write_create_trigger(out: &mut String, trig: &TriggerInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: TRIGGER\n--\n\n",
        trig.name
    ));
    let def = trig.definition.trim_end();
    out.push_str(def);
    if !def.ends_with(';') {
        out.push(';');
    }
    out.push('\n');
}

/// Write ALTER TABLE ... DISABLE TRIGGER ALL for tables that have all triggers disabled.
///
/// This is emitted once per table when tgenabled='D' for all triggers on that table.
pub fn write_disable_trigger_all(out: &mut String, schema: &str, table: &str) {
    let table_qname = format!("{}.{}", quote_ident(schema), quote_ident(table));
    out.push_str(&format!(
        "\n--\n-- Name: {table}; Type: TABLE TRIGGER DISABLE\n--\n\nALTER TABLE {table_qname} DISABLE TRIGGER ALL;\n"
    ));
}

/// Write a CREATE EVENT TRIGGER statement.
pub fn write_create_event_trigger(out: &mut String, et: &EventTriggerInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: EVENT TRIGGER\n--\n\n",
        et.name
    ));
    let func_qname = format!(
        "{}.{}",
        quote_ident(&et.func_schema),
        quote_ident(&et.func_name)
    );
    let name_q = quote_ident(&et.name);
    if et.tags.is_empty() {
        out.push_str(&format!(
            "CREATE EVENT TRIGGER {name_q} ON {}\n    EXECUTE FUNCTION {func_qname}();\n",
            et.event
        ));
    } else {
        out.push_str(&format!(
            "CREATE EVENT TRIGGER {name_q} ON {}\n    WHEN TAG IN ({})\n    EXECUTE FUNCTION {func_qname}();\n",
            et.event, et.tags
        ));
    }
}

/// Write extended statistics DDL (CREATE STATISTICS + optional ALTER STATISTICS).
pub fn write_create_extended_statistics(out: &mut String, stat: &ExtendedStatInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: STATISTICS\n--\n\n",
        stat.name
    ));
    let def = stat.definition.trim_end();
    out.push_str(def);
    if !def.ends_with(';') {
        out.push(';');
    }
    out.push('\n');

    // If stattarget is explicitly set (not NULL/default), emit ALTER STATISTICS.
    if let Some(target) = stat.stattarget {
        if target >= 0 {
            let qname = format!("{}.{}", quote_ident(&stat.schema), quote_ident(&stat.name));
            out.push_str(&format!(
                "ALTER STATISTICS {qname} SET STATISTICS {};\n",
                target
            ));
        }
    }
}

/// Write a CREATE TRANSFORM statement.
pub fn write_create_transform(out: &mut String, tr: &TransformInfo) {
    out.push_str(&format!(
        "--\n-- Name: TRANSFORM FOR {}; Type: TRANSFORM\n--\n\n",
        tr.type_name
    ));
    out.push_str(&format!(
        "CREATE TRANSFORM FOR {} LANGUAGE {}\n(\n",
        tr.type_name,
        quote_ident(&tr.lang_name)
    ));
    if !tr.fromsql.is_empty() {
        out.push_str(&format!("    FROM SQL WITH FUNCTION {},\n", tr.fromsql));
    }
    if !tr.tosql.is_empty() {
        out.push_str(&format!("    TO SQL WITH FUNCTION {}\n", tr.tosql));
    }
    out.push_str(");\n");
}

/// Write COMMENT ON TYPE statements.
pub fn write_type_comments(out: &mut String, comments: &[TypeCommentInfo]) {
    for c in comments {
        let escaped = c.comment.replace('\'', "''");
        out.push_str(&format!(
            "COMMENT ON TYPE {} IS '{}';\n",
            c.type_name, escaped
        ));
    }
}

/// Write a `CREATE FOREIGN DATA WRAPPER` statement.
pub fn write_create_fdw(out: &mut String, fdw: &FdwInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: FOREIGN DATA WRAPPER\n--\n\n",
        fdw.name
    ));
    out.push_str(&format!(
        "CREATE FOREIGN DATA WRAPPER {}",
        quote_ident(&fdw.name)
    ));
    if !fdw.handler.is_empty() {
        out.push_str(&format!(" HANDLER {}", quote_ident(&fdw.handler)));
    }
    if !fdw.validator.is_empty() {
        out.push_str(&format!(" VALIDATOR {}", quote_ident(&fdw.validator)));
    }
    let opts = format_fdw_options(&fdw.options);
    if !opts.is_empty() {
        out.push_str(&format!(" OPTIONS ({opts})"));
    }
    out.push_str(";\n");
}

/// Write `ALTER FOREIGN DATA WRAPPER … OWNER TO …`.
pub fn write_alter_fdw_owner(out: &mut String, fdw: &FdwInfo) {
    out.push_str(&format!(
        "ALTER FOREIGN DATA WRAPPER {} OWNER TO {};\n",
        quote_ident(&fdw.name),
        quote_ident(&fdw.owner)
    ));
}

/// Write a `CREATE SERVER` statement.
pub fn write_create_foreign_server(out: &mut String, srv: &ForeignServerInfo) {
    out.push_str(&format!("--\n-- Name: {}; Type: SERVER\n--\n\n", srv.name));
    out.push_str(&format!("CREATE SERVER {}", quote_ident(&srv.name)));
    if !srv.server_type.is_empty() {
        out.push_str(&format!(" TYPE '{}'", srv.server_type));
    }
    if !srv.server_version.is_empty() {
        out.push_str(&format!(" VERSION '{}'", srv.server_version));
    }
    out.push_str(&format!(
        " FOREIGN DATA WRAPPER {}",
        quote_ident(&srv.fdw_name)
    ));
    let opts = format_fdw_options(&srv.options);
    if !opts.is_empty() {
        out.push_str(&format!(" OPTIONS ({opts})"));
    }
    out.push_str(";\n");
}

/// Write `ALTER SERVER … OWNER TO …`.
pub fn write_alter_server_owner(out: &mut String, srv: &ForeignServerInfo) {
    out.push_str(&format!(
        "ALTER SERVER {} OWNER TO {};\n",
        quote_ident(&srv.name),
        quote_ident(&srv.owner)
    ));
}

/// Write a `CREATE FOREIGN TABLE` statement.
pub fn write_create_foreign_table(out: &mut String, ft: &ForeignTableInfo) {
    let qname = ft.qualified_name();
    out.push_str(&format!(
        "--\n-- Name: {}; Type: FOREIGN TABLE\n--\n\n",
        ft.name
    ));
    out.push_str(&format!("CREATE FOREIGN TABLE {qname} (\n"));
    for (i, col) in ft.columns.iter().enumerate() {
        out.push_str(&format!("    {} {}", quote_ident(&col.name), col.type_name));
        if col.not_null {
            out.push_str(" NOT NULL");
        }
        if let Some(ref default) = col.default_expr {
            out.push_str(&format!(" DEFAULT {default}"));
        }
        if i + 1 < ft.columns.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str(&format!(")\nSERVER {};\n", quote_ident(&ft.server_name)));
}

/// Write `ALTER FOREIGN TABLE … OWNER TO …`.
pub fn write_alter_foreign_table_owner(out: &mut String, ft: &ForeignTableInfo) {
    let qname = ft.qualified_name();
    out.push_str(&format!(
        "ALTER FOREIGN TABLE {qname} OWNER TO {};\n",
        quote_ident(&ft.owner)
    ));
}

/// Write `ALTER FOREIGN TABLE … ALTER COLUMN … OPTIONS` for columns with FDW options.
pub fn write_alter_foreign_table_column_options(out: &mut String, ft: &ForeignTableInfo) {
    let qname = ft.qualified_name();
    for col in &ft.columns {
        if col.options_raw.is_empty() {
            continue;
        }
        let formatted = format_fdw_options(&col.options_raw);
        if !formatted.is_empty() {
            out.push_str(&format!(
                "ALTER FOREIGN TABLE {qname} ALTER COLUMN {} OPTIONS ({formatted});\n",
                quote_ident(&col.name)
            ));
        }
    }
}

/// Write a `CREATE USER MAPPING` statement.
pub fn write_create_user_mapping(out: &mut String, um: &UserMappingInfo) {
    out.push_str(&format!(
        "--\n-- Name: USER MAPPING {} {}; Type: USER MAPPING\n--\n\n",
        um.username, um.server_name
    ));
    let user_clause = if um.username == "PUBLIC" {
        "PUBLIC".to_string()
    } else {
        quote_ident(&um.username)
    };
    out.push_str(&format!(
        "CREATE USER MAPPING FOR {user_clause} SERVER {}",
        quote_ident(&um.server_name)
    ));
    let opts = format_fdw_options(&um.options);
    if !opts.is_empty() {
        out.push_str(&format!(" OPTIONS ({opts})"));
    }
    out.push_str(";\n");
}

/// Write a `CREATE PUBLICATION` statement.
pub fn write_create_publication(out: &mut String, pub_info: &PublicationInfo) {
    out.push_str(&format!(
        "--\n-- Name: {}; Type: PUBLICATION\n--\n\n",
        pub_info.name
    ));
    out.push_str(&format!(
        "CREATE PUBLICATION {}",
        quote_ident(&pub_info.name)
    ));
    if pub_info.all_tables {
        out.push_str(" FOR ALL TABLES");
    }
    // Emit WITH clause only if publish settings differ from defaults.
    let all_default =
        pub_info.pub_insert && pub_info.pub_update && pub_info.pub_delete && pub_info.pub_truncate;
    if !all_default {
        let mut ops = Vec::new();
        if pub_info.pub_insert {
            ops.push("insert");
        }
        if pub_info.pub_update {
            ops.push("update");
        }
        if pub_info.pub_delete {
            ops.push("delete");
        }
        if pub_info.pub_truncate {
            ops.push("truncate");
        }
        out.push_str(&format!(" WITH (publish = '{}')", ops.join(", ")));
    }
    out.push_str(";\n");
}

/// Write `ALTER PUBLICATION … OWNER TO …`.
pub fn write_alter_publication_owner(out: &mut String, pub_info: &PublicationInfo) {
    out.push_str(&format!(
        "ALTER PUBLICATION {} OWNER TO {};\n",
        quote_ident(&pub_info.name),
        quote_ident(&pub_info.owner)
    ));
}

/// Write `ALTER PUBLICATION … ADD TABLE` and `ADD TABLES IN SCHEMA` statements.
pub fn write_alter_publication_tables(out: &mut String, pub_info: &PublicationInfo) {
    for table in &pub_info.tables {
        let tqname = format!(
            "{}.{}",
            quote_ident(&table.schema),
            quote_ident(&table.name)
        );
        out.push_str(&format!(
            "ALTER PUBLICATION {} ADD TABLE ONLY {tqname}",
            quote_ident(&pub_info.name)
        ));
        if !table.where_clause.is_empty() {
            out.push_str(&format!(" WHERE ({})", table.where_clause));
        }
        out.push_str(";\n");
    }
    for schema in &pub_info.schemas {
        out.push_str(&format!(
            "ALTER PUBLICATION {} ADD TABLES IN SCHEMA {};\n",
            quote_ident(&pub_info.name),
            quote_ident(schema)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_copy_value_basic() {
        assert_eq!(escape_copy_value("hello"), "hello");
        assert_eq!(escape_copy_value("a\tb"), "a\\tb");
        assert_eq!(escape_copy_value("a\\b"), "a\\\\b");
        assert_eq!(escape_copy_value("a\nb"), "a\\nb");
    }
}
