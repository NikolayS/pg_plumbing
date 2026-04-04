// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_dump implementation.

pub mod catalog;
pub mod custom_format;
pub mod directory_format;
pub mod filter;
pub mod format;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

/// Options controlling what and how to dump.
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Bare database name (used for DDL: CREATE DATABASE, \connect).
    pub dbname: String,
    /// Full conninfo string (used for the actual connection).
    pub conninfo: String,
    /// Tables to include (empty = all).
    pub tables: Vec<String>,
    /// Dump only the schema, no data.
    pub schema_only: bool,
    /// Dump only the data, no schema.
    pub data_only: bool,
    /// Use INSERT statements instead of COPY.
    pub inserts: bool,
    /// Include column names in INSERT statements.
    pub column_inserts: bool,
    /// Bundle multiple rows per INSERT statement.
    pub rows_per_insert: Option<u32>,
    /// Schemas to include (empty = all).
    pub schemas: Vec<String>,
    /// Schemas to exclude.
    pub exclude_schemas: Vec<String>,
    /// Tables to exclude.
    pub exclude_tables: Vec<String>,
    /// Tables whose data should be excluded (schema dumped, data skipped).
    pub exclude_table_data: Vec<String>,
    /// Suppress ownership statements.
    pub no_owner: bool,
    /// Suppress privilege statements.
    pub no_privileges: bool,
    /// Number of parallel dump workers (1 = sequential).
    pub jobs: usize,
    /// Prepend DROP statements before CREATE statements.
    pub clean: bool,
    /// Use DROP ... IF EXISTS (only meaningful with clean).
    pub if_exists: bool,
    /// Include CREATE DATABASE + \connect at the start.
    pub create_db: bool,
}

/// Dump a database in plain SQL format.
pub async fn dump_plain(opts: &DumpOptions) -> Result<String> {
    let (client, connection) = tokio_postgres::connect(&opts.conninfo, NoTls)
        .await
        .with_context(|| format!("failed to connect to database \"{}\"", opts.dbname))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let tables = catalog::get_tables(&client, opts)
        .await
        .context("failed to query catalog")?;
    let sequences = catalog::get_sequences(&client, opts)
        .await
        .context("failed to query sequences")?;
    let views = catalog::get_views(&client, opts)
        .await
        .context("failed to query views")?;
    let schemas = catalog::get_schemas(&client, opts)
        .await
        .context("failed to query schemas")?;

    let mut out = String::new();

    // Header
    out.push_str("--\n");
    out.push_str("-- PostgreSQL database dump\n");
    out.push_str("--\n\n");

    // --create: emit CREATE DATABASE + \connect before SET commands.
    // Include ENCODING, LC_COLLATE, LC_CTYPE from pg_database if available.
    if opts.create_db {
        let dbname = &opts.dbname;
        // Query encoding and locale settings from the catalog.
        let db_info = client
            .query_opt(
                "SELECT pg_encoding_to_char(encoding) AS encoding, \
                        datcollate, datctype \
                 FROM pg_catalog.pg_database \
                 WHERE datname = $1",
                &[dbname],
            )
            .await
            .ok()
            .flatten();

        if let Some(row) = db_info {
            let encoding: &str = row.get("encoding");
            let collate: &str = row.get("datcollate");
            let ctype: &str = row.get("datctype");
            out.push_str(&format!(
                "CREATE DATABASE \"{dbname}\" WITH ENCODING = '{encoding}' LC_COLLATE = '{collate}' LC_CTYPE = '{ctype}';\n"
            ));
        } else {
            // Encoding/locale info not available — use defaults.
            // NOTE: the created database will inherit server defaults for
            // ENCODING, LC_COLLATE, and LC_CTYPE. For a faithful restore,
            // ensure the target server uses matching locale settings.
            out.push_str(&format!("CREATE DATABASE \"{dbname}\";\n"));
        }
        out.push_str(&format!("\\connect \"{dbname}\"\n\n"));
    }

    out.push_str("SET statement_timeout = 0;\n");
    out.push_str("SET lock_timeout = 0;\n");
    out.push_str("SET idle_in_transaction_session_timeout = 0;\n");
    out.push_str("SET client_encoding = 'UTF8';\n");
    out.push_str("SET standard_conforming_strings = on;\n");
    out.push_str("SELECT pg_catalog.set_config('search_path', '', false);\n");
    out.push_str("SET check_function_bodies = false;\n");
    out.push_str("SET xmloption = content;\n");
    out.push_str("SET client_min_messages = warning;\n");
    out.push_str("SET row_security = off;\n\n");

    // When a table filter is active (-t), we only want objects directly tied
    // to the selected tables.  Views and schema OWNER TO statements reference
    // objects that may not exist in the restore target, so we skip them.
    // Sequences are only emitted when owned by one of the selected tables.
    let table_filter_active = !opts.tables.is_empty();

    // Set of table names actually being dumped — used to filter sequences.
    let dumped_table_names: std::collections::HashSet<&str> =
        tables.iter().map(|t| t.name.as_str()).collect();

    // Compute schemas that have at least one table being dumped.
    let dumped_schema_names: std::collections::HashSet<&str> =
        tables.iter().map(|t| t.schema.as_str()).collect();

    if !opts.data_only {
        for seq in &sequences {
            if table_filter_active {
                // In table-filter mode, only emit sequences owned by a
                // selected table.  Standalone sequences and sequences owned
                // by other tables are skipped.
                match &seq.owned_by_table {
                    Some(t) if dumped_table_names.contains(t.as_str()) => {}
                    _ => continue,
                }
            } else {
                // Full-DB or schema-filter dump: skip sequences from schemas
                // not represented in this dump.
                if !dumped_schema_names.is_empty()
                    && !dumped_schema_names.contains(seq.schema.as_str())
                {
                    continue;
                }
            }
            format::write_create_sequence(&mut out, seq);
            out.push('\n');
            format::write_alter_sequence(&mut out, seq);
        }

        // Emit ALTER SCHEMA ... OWNER TO only for full-DB or schema-filtered
        // dumps.  A table-specific dump (-t) must not emit schema ownership
        // because the schema may not exist in the restore target.
        if !table_filter_active {
            let mut emitted_schema = false;
            for schema in &schemas {
                if dumped_schema_names.contains(schema.name.as_str()) {
                    format::write_alter_schema_owner(&mut out, schema);
                    emitted_schema = true;
                }
            }
            if emitted_schema {
                out.push('\n');
            }
        }
    }

    for table in &tables {
        if !opts.data_only {
            // --clean: emit DROP statement before each CREATE TABLE
            if opts.clean {
                let qname = table.qualified_name();
                if opts.if_exists {
                    out.push_str(&format!("DROP TABLE IF EXISTS {qname} CASCADE;\n"));
                } else {
                    out.push_str(&format!("DROP TABLE {qname} CASCADE;\n"));
                }
            }
            format::write_create_table(&mut out, table);
            format::write_alter_table_owner(&mut out, table);
            out.push('\n');
        }

        if !opts.schema_only {
            // Skip data for tables matching --exclude-table-data patterns.
            let skip_data =
                filter::matches_any(&opts.exclude_table_data, &table.schema, &table.name);
            if !skip_data {
                format::write_table_data(&mut out, &client, table, opts).await?;
                out.push('\n');
            }
        }
    }

    if !opts.data_only && !table_filter_active {
        // Skip views entirely in table-filter mode: a view may reference
        // tables that are not included in the dump, causing restore failures.
        for view in &views {
            if !dumped_schema_names.is_empty()
                && !dumped_schema_names.contains(view.schema.as_str())
            {
                continue;
            }
            format::write_create_view(&mut out, view);
            out.push('\n');
        }
    }

    out.push_str("--\n");
    out.push_str("-- PostgreSQL database dump complete\n");
    out.push_str("--\n\n");

    // Apply --no-owner / --no-acl filters.
    let out = apply_output_filters(&out, opts.no_owner, opts.no_privileges);

    Ok(out)
}

/// Filter a SQL dump string according to `--no-owner` / `--no-acl` options.
///
/// Each line of `sql` is tested against simple prefix patterns:
///
/// * `no_owner`: drops lines that begin with `ALTER … OWNER TO `.
///   Real pg_dump emits these as single-line statements.
/// * `no_privileges`: drops lines that begin with `GRANT ` or `REVOKE `.
fn apply_output_filters(sql: &str, no_owner: bool, no_privileges: bool) -> String {
    if !no_owner && !no_privileges {
        return sql.to_string();
    }
    sql.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            if no_owner && is_owner_line(trimmed) {
                return false;
            }
            if no_privileges && is_privilege_line(trimmed) {
                return false;
            }
            true
        })
        .map(|l| format!("{l}\n"))
        .collect()
}

/// Return true for `ALTER <keyword> … OWNER TO …;` lines.
fn is_owner_line(s: &str) -> bool {
    if !s.starts_with("ALTER ") {
        return false;
    }
    // Efficient scan: look for " OWNER TO " substring.
    s.contains(" OWNER TO ")
}

/// Return true for `GRANT …` or `REVOKE …` lines.
fn is_privilege_line(s: &str) -> bool {
    s.starts_with("GRANT ") || s.starts_with("REVOKE ")
}

/// Dump a database in PostgreSQL custom archive format.
///
/// Writes the binary `.dump` file to the given `Vec<u8>`.
pub async fn dump_custom(opts: &DumpOptions) -> Result<Vec<u8>> {
    use catalog::quote_ident;
    use custom_format::{write_data_block, write_eof, write_header, write_toc_entry, TocEntry};
    use format::write_table_data_to_string;

    let (client, connection) = tokio_postgres::connect(&opts.conninfo, NoTls)
        .await
        .with_context(|| format!("failed to connect to database \"{}\"", opts.dbname))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let tables = catalog::get_tables(&client, opts)
        .await
        .context("failed to query catalog")?;

    // ── Build TOC entries ───────────────────────────────────────────────────
    let mut schema_entries: Vec<TocEntry> = Vec::new();
    let mut next_id = 1i32;

    for table in &tables {
        if !opts.data_only {
            // Query sequences owned by this table's columns.
            let seq_ddls = get_sequences_for_table(&client, table).await?;
            for (seq_name, seq_ddl) in seq_ddls {
                let entry =
                    TocEntry::schema(next_id, &seq_name, "SEQUENCE", &seq_ddl, &table.schema);
                schema_entries.push(entry);
                next_id += 1;
            }

            // Schema entry: CREATE TABLE
            let mut ddl = String::new();
            format::write_create_table(&mut ddl, table);
            let entry = TocEntry::schema(next_id, &table.name, "TABLE", &ddl, &table.schema);
            schema_entries.push(entry);
            next_id += 1;
        }
    }

    // Collect data: build COPY strings for each table.
    let mut table_data: Vec<(TocEntry, Vec<u8>)> = Vec::new();

    if !opts.schema_only {
        // Map namespace.tag → schema dump_id for dependencies.
        // Use qualified key to avoid collision when two schemas have same-named tables.
        let schema_id_map: std::collections::HashMap<String, i32> = schema_entries
            .iter()
            .map(|e| (format!("{}.{}", e.namespace, e.tag), e.dump_id))
            .collect();

        for table in &tables {
            // Skip data for tables matching --exclude-table-data patterns.
            if filter::matches_any(&opts.exclude_table_data, &table.schema, &table.name) {
                continue;
            }

            let qname = table.qualified_name();
            let col_names: Vec<String> =
                table.columns.iter().map(|c| quote_ident(&c.name)).collect();
            let col_list = col_names.join(", ");
            let copy_stmt = format!("COPY {qname} ({col_list}) FROM stdin;");

            let qualified_key = format!("{}.{}", table.schema, table.name);
            let deps = if let Some(&sid) = schema_id_map.get(&qualified_key) {
                vec![sid]
            } else {
                Vec::new()
            };

            let entry = TocEntry::data(next_id, &table.name, &copy_stmt, &table.schema, deps);

            // Collect data as COPY text (without the COPY header and \.).
            let data_str = write_table_data_to_string(&client, table, opts).await?;
            table_data.push((entry, data_str.into_bytes()));
            next_id += 1;
        }
    }

    // ── Two-pass write ──────────────────────────────────────────────────────
    // Pass 1: write header + TOC with placeholder offsets,
    //         then write data blocks while tracking offsets.
    // We use an in-memory Vec<u8> so we can seek back to patch offsets if needed.
    // For simplicity, we do a single pass: write header + TOC, then data.
    // Since we write data sequentially we record the offset before each block
    // by tracking the current byte count.

    let toc_count = schema_entries.len() + table_data.len();

    // Compute byte length of header + all TOC entries so we know the offset
    // at which data blocks begin. We do this by serialising the header+TOC
    // into a temporary buffer first (with placeholder data_offset = 0),
    // then append data blocks and patch the offsets in the TOC.

    // ── Stage 1: build TOC (data offsets unknown) ──────────────────────────
    let mut toc_buf: Vec<u8> = Vec::new();
    write_header(&mut toc_buf, toc_count).map_err(|e| anyhow::anyhow!("write header: {e}"))?;

    // Write schema entries first.
    for entry in &schema_entries {
        write_toc_entry(&mut toc_buf, entry)
            .map_err(|e| anyhow::anyhow!("write TOC entry: {e}"))?;
    }

    // Write data entries with placeholder offset=0.
    let mut data_entry_offsets: Vec<usize> = Vec::new(); // byte position of each data TOC entry

    for (entry, _) in &table_data {
        data_entry_offsets.push(toc_buf.len());
        write_toc_entry(&mut toc_buf, entry)
            .map_err(|e| anyhow::anyhow!("write data TOC entry: {e}"))?;
    }

    // ── Stage 2: write data blocks, tracking offsets ───────────────────────
    let header_toc_len = toc_buf.len();
    let mut data_buf: Vec<u8> = Vec::new();

    let mut block_offsets: Vec<u64> = Vec::new();
    for (entry, data) in &table_data {
        let offset = (header_toc_len + data_buf.len()) as u64;
        block_offsets.push(offset);
        write_data_block(&mut data_buf, entry.dump_id, data)
            .map_err(|e| anyhow::anyhow!("write data block: {e}"))?;
    }

    write_eof(&mut data_buf).map_err(|e| anyhow::anyhow!("write EOF: {e}"))?;

    // ── Stage 3: patch data offsets into the TOC buffer ───────────────────
    // We need to find where in toc_buf each data TOC entry's data_offset field
    // lives and patch it. The data_offset is the last field in each TOC entry:
    //   flag(1) + u64(8) = 9 bytes (or flag(1) + [0u8;8] = 9 bytes for "invalid").
    // We re-encode the entire data TOC section with correct offsets.

    // Simpler: rebuild the data TOC entries with correct offsets and replace
    // the relevant portion of toc_buf.

    let mut patched_data_toc: Vec<u8> = Vec::new();
    for (i, (entry, _)) in table_data.iter().enumerate() {
        let mut patched_entry = entry.clone();
        patched_entry.data_offset = block_offsets[i];
        write_toc_entry(&mut patched_data_toc, &patched_entry)
            .map_err(|e| anyhow::anyhow!("write patched TOC entry: {e}"))?;
    }

    // Replace the data TOC portion in toc_buf.
    let data_toc_start_pos = data_entry_offsets.first().copied().unwrap_or(toc_buf.len());
    toc_buf.truncate(data_toc_start_pos);
    toc_buf.extend_from_slice(&patched_data_toc);

    // ── Combine ────────────────────────────────────────────────────────────
    let mut output = toc_buf;
    output.extend_from_slice(&data_buf);

    Ok(output)
}

/// Query sequences owned by columns of the given table and return DDL for each.
async fn get_sequences_for_table(
    client: &tokio_postgres::Client,
    table: &catalog::TableInfo,
) -> Result<Vec<(String, String)>> {
    // Find sequences owned by this table via pg_depend.
    let rows = client
        .query(
            "SELECT s.relname AS seq_name, \
                    n.nspname AS seq_schema, \
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace tn ON tn.oid = c.relnamespace \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 \
             JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             JOIN pg_catalog.pg_depend dep ON dep.refobjid = c.oid \
                  AND dep.classid = 'pg_catalog.pg_class'::regclass \
                  AND dep.refclassid = 'pg_catalog.pg_class'::regclass \
                  AND dep.deptype = 'a' \
             JOIN pg_catalog.pg_class s ON s.oid = dep.objid AND s.relkind = 'S' \
             JOIN pg_catalog.pg_namespace n ON n.oid = s.relnamespace \
             WHERE c.relkind = 'r' \
               AND tn.nspname = $1 \
               AND c.relname = $2",
            &[&table.schema, &table.name],
        )
        .await
        .context("query sequences")?;

    let mut result = Vec::new();
    for row in &rows {
        let seq_name: &str = row.get("seq_name");
        let seq_schema: &str = row.get("seq_schema");

        // Build CREATE SEQUENCE DDL.
        let ddl = format!(
            "--\n-- Name: {seq_name}; Type: SEQUENCE\n--\n\nCREATE SEQUENCE {seq_schema}.{seq_name}\n    START WITH 1\n    INCREMENT BY 1\n    NO MINVALUE\n    NO MAXVALUE\n    CACHE 1;\n"
        );
        result.push((seq_name.to_string(), ddl));
    }
    Ok(result)
}

#[cfg(test)]
mod filter_tests {
    use super::{apply_output_filters, is_owner_line, is_privilege_line};

    #[test]
    fn is_owner_line_detects_alter_owner() {
        assert!(is_owner_line("ALTER TABLE public.foo OWNER TO postgres;"));
        assert!(is_owner_line(
            "ALTER SEQUENCE public.foo_id_seq OWNER TO alice;"
        ));
        assert!(is_owner_line("ALTER VIEW public.v OWNER TO bob;"));
        assert!(is_owner_line("ALTER SCHEMA public OWNER TO postgres;"));
        assert!(!is_owner_line("CREATE TABLE public.foo (id int);"));
        assert!(!is_owner_line("GRANT SELECT ON TABLE foo TO bar;"));
        assert!(!is_owner_line("ALTER TABLE foo ADD COLUMN bar int;"));
    }

    #[test]
    fn is_privilege_line_detects_grant_revoke() {
        assert!(is_privilege_line("GRANT SELECT ON TABLE foo TO bar;"));
        assert!(is_privilege_line("REVOKE ALL ON TABLE foo FROM public;"));
        assert!(!is_privilege_line("CREATE TABLE foo (id int);"));
        assert!(!is_privilege_line("ALTER TABLE foo OWNER TO postgres;"));
    }

    #[test]
    fn apply_output_filters_no_flags() {
        let sql = "CREATE TABLE foo (id int);\nALTER TABLE foo OWNER TO postgres;\nGRANT SELECT ON foo TO bar;\n";
        // No flags: pass through unchanged.
        assert_eq!(apply_output_filters(sql, false, false), sql);
    }

    #[test]
    fn apply_output_filters_no_owner() {
        let sql = "CREATE TABLE foo (id int);\nALTER TABLE foo OWNER TO postgres;\nGRANT SELECT ON foo TO bar;\n";
        let out = apply_output_filters(sql, true, false);
        assert!(out.contains("CREATE TABLE"), "CREATE TABLE should remain");
        assert!(!out.contains("OWNER TO"), "OWNER TO should be stripped");
        assert!(out.contains("GRANT SELECT"), "GRANT should remain");
    }

    #[test]
    fn apply_output_filters_no_privileges() {
        let sql = "CREATE TABLE foo (id int);\nALTER TABLE foo OWNER TO postgres;\nGRANT SELECT ON foo TO bar;\nREVOKE ALL ON foo FROM public;\n";
        let out = apply_output_filters(sql, false, true);
        assert!(out.contains("CREATE TABLE"), "CREATE TABLE should remain");
        assert!(out.contains("OWNER TO"), "OWNER TO should remain");
        assert!(!out.contains("GRANT "), "GRANT should be stripped");
        assert!(!out.contains("REVOKE "), "REVOKE should be stripped");
    }

    #[test]
    fn apply_output_filters_both() {
        let sql = "CREATE TABLE foo (id int);\nALTER TABLE foo OWNER TO postgres;\nGRANT SELECT ON foo TO bar;\nREVOKE ALL ON foo FROM public;\n";
        let out = apply_output_filters(sql, true, true);
        assert!(out.contains("CREATE TABLE"), "CREATE TABLE should remain");
        assert!(!out.contains("OWNER TO"), "OWNER TO should be stripped");
        assert!(!out.contains("GRANT "), "GRANT should be stripped");
        assert!(!out.contains("REVOKE "), "REVOKE should be stripped");
    }
}
