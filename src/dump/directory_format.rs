// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Directory format dump (`pg_dump -F directory`).
//!
//! Creates a directory containing:
//!   - `toc.dat` — text TOC listing schema DDL and data file references
//!   - `<N>.dat`  — one plain COPY-format file per table with data

use anyhow::{bail, Context, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_postgres::{Client, NoTls};

use super::catalog;
use super::format;
use super::DumpOptions;

/// Replace characters that are invalid or problematic in filenames with `_`.
///
/// Handles: `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`, space, `.`, and null bytes.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | '.' | '\0' => '_',
            _ => c,
        })
        .collect()
}

/// Dump a database in directory format.
///
/// Creates `output_dir` (errors if it already exists and is non-empty),
/// writes `toc.dat` and one `<N>.dat` per table.
///
/// When `opts.jobs > 1`, table data files are written in parallel using
/// up to `opts.jobs` concurrent tokio tasks.
pub async fn dump_directory(opts: &DumpOptions, output_dir: &str) -> Result<()> {
    // Create the output directory; fail if it already exists and is non-empty.
    let dir_path = std::path::Path::new(output_dir);
    if dir_path.exists() {
        let entries: Vec<_> = std::fs::read_dir(dir_path)
            .context("failed to read output directory")?
            .collect();
        if !entries.is_empty() {
            bail!("directory \"{}\" exists and is not empty", output_dir);
        }
    } else {
        std::fs::create_dir_all(dir_path)
            .with_context(|| format!("failed to create directory \"{}\"", output_dir))?;
    }

    // Connect to the database (for schema + sequential data path).
    let conninfo = crate::build_conninfo(&opts.dbname);
    let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
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

    // Build the TOC and per-table data files.
    let mut toc = String::new();

    toc.push_str("; pg_plumbing directory format TOC\n");
    toc.push_str(&format!("; dbname: {}\n", opts.dbname));
    toc.push_str(";\n");

    // Schema section — always single-threaded.
    if !opts.data_only {
        // 1. Emit CREATE SCHEMA IF NOT EXISTS for every non-public schema first.
        let mut seen_schemas: std::collections::HashSet<String> = std::collections::HashSet::new();
        for table in &tables {
            if table.schema != "public" && seen_schemas.insert(table.schema.clone()) {
                let schema_ddl = format!(
                    "CREATE SCHEMA IF NOT EXISTS {};\n",
                    catalog::quote_ident(&table.schema)
                );
                let schema_file = format!("{}.schema.ddl", table.schema);
                let schema_path = dir_path.join(&schema_file);
                std::fs::write(&schema_path, &schema_ddl)
                    .with_context(|| format!("failed to write {schema_file}"))?;
                toc.push_str(&format!("SCHEMA {} {schema_file}\n", table.schema));
            }
        }

        // 2. Emit CREATE TYPE for all enum types used by tables.
        let enum_types = get_enum_types(&client).await?;
        for (type_schema, type_name, type_ddl) in &enum_types {
            let type_key = format!("{type_schema}.{type_name}");
            let safe_schema = sanitize_filename(type_schema);
            let safe_name = sanitize_filename(type_name);
            let type_file = format!("{safe_schema}__{safe_name}.type.ddl");
            let type_path = dir_path.join(&type_file);
            std::fs::write(&type_path, type_ddl)
                .with_context(|| format!("failed to write {type_file}"))?;
            toc.push_str(&format!("TYPE {type_key} {type_file}\n"));
        }

        // 3. Emit table DDL (partitioned parent tables before their children).
        // Track sequences already emitted so that a sequence shared between
        // multiple tables (duplicate `deptype='a'` rows in pg_depend) is only
        // written once.  Duplicate TOC entries would cause CREATE SEQUENCE to
        // run twice on restore, producing "ERROR: relation already exists".
        // Fixes: issue #21.
        let mut emitted_sequences: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for table in &tables {
            // Dump any sequences that this table's columns depend on first.
            let sequences = get_table_sequences(&client, table).await?;
            for (seq_schema, seq_name, seq_ddl) in &sequences {
                let seq_key = format!("{seq_schema}.{seq_name}");

                // Skip if we already emitted this sequence for a prior table.
                if !emitted_sequences.insert(seq_key.clone()) {
                    continue; // already emitted this sequence
                }

                let safe_schema = sanitize_filename(seq_schema);
                let safe_name = sanitize_filename(seq_name);
                let seq_file = format!("{safe_schema}__{safe_name}.seq.ddl");
                let seq_path = dir_path.join(&seq_file);
                std::fs::write(&seq_path, seq_ddl)
                    .with_context(|| format!("failed to write {seq_file}"))?;
                toc.push_str(&format!("SEQUENCE {seq_key} {seq_file}\n"));
            }

            let mut ddl = String::new();
            format::write_create_table(&mut ddl, table);
            let ddl_file = format!("{}.ddl", table.name);
            let ddl_path = dir_path.join(&ddl_file);
            std::fs::write(&ddl_path, &ddl)
                .with_context(|| format!("failed to write {ddl_file}"))?;
            toc.push_str(&format!("TABLE {} {ddl_file}\n", table.qualified_name()));
        }
    }

    // Data section — parallel when jobs > 1.
    if !opts.schema_only {
        // Build owned (idx, table, dat_file) tuples up-front for deterministic TOC order.
        // Skip partitioned parent tables (relkind='p') — they hold no rows directly.
        let data_entries: Vec<(usize, catalog::TableInfo, String)> = tables
            .into_iter()
            .filter(|t| t.partition_key.is_none()) // skip partitioned parents
            .enumerate()
            .map(|(idx, t)| (idx, t, format!("{}.dat", idx + 1)))
            .collect();

        if opts.jobs <= 1 {
            // Sequential path — reuse the existing single connection.
            for (_, table, dat_file) in &data_entries {
                // Skip data for tables matching --exclude-table-data patterns.
                if super::filter::matches_any(&opts.exclude_table_data, &table.schema, &table.name)
                {
                    continue;
                }

                let dat_path = dir_path.join(dat_file);
                let mut data_buf = String::new();
                format::write_table_data(&mut data_buf, &client, table, opts).await?;

                if !data_buf.is_empty() {
                    std::fs::write(&dat_path, &data_buf)
                        .with_context(|| format!("failed to write {dat_file}"))?;
                    toc.push_str(&format!("DATA {} {dat_file}\n", table.qualified_name()));
                }
            }
        } else {
            // Parallel path — spawn N tasks, each with its own DB connection.
            let jobs = opts.jobs;
            let conninfo = crate::build_conninfo(&opts.dbname);
            let semaphore = Arc::new(Semaphore::new(jobs));
            let dir_path_owned = dir_path.to_path_buf();
            let opts_arc = Arc::new(opts.clone());

            let tables_owned: Vec<(usize, catalog::TableInfo, String)> = data_entries;

            // Spawn one task per table; each acquires a semaphore permit.
            let mut join_handles = Vec::new();
            for (idx, table, dat_file) in tables_owned {
                // Skip data for tables matching --exclude-table-data patterns.
                if super::filter::matches_any(
                    &opts_arc.exclude_table_data,
                    &table.schema,
                    &table.name,
                ) {
                    continue;
                }

                let sem = Arc::clone(&semaphore);
                let conninfo = conninfo.clone();
                let dir_path = dir_path_owned.clone();
                let opts_clone = Arc::clone(&opts_arc);

                let handle = tokio::task::spawn(async move {
                    let _permit = sem.acquire().await.expect("semaphore closed");

                    // Each worker opens its own connection.
                    let (worker_client, conn) = tokio_postgres::connect(&conninfo, NoTls)
                        .await
                        .with_context(|| {
                            format!(
                                "parallel worker: failed to connect for table {}",
                                table.name
                            )
                        })?;

                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            eprintln!("parallel worker connection error: {e}");
                        }
                    });

                    let mut data_buf = String::new();
                    format::write_table_data(&mut data_buf, &worker_client, &table, &opts_clone)
                        .await?;

                    let wrote_data = if !data_buf.is_empty() {
                        let dat_path = dir_path.join(&dat_file);
                        std::fs::write(&dat_path, &data_buf)
                            .with_context(|| format!("failed to write {dat_file}"))?;
                        true
                    } else {
                        false
                    };

                    Ok::<(String, String, bool), anyhow::Error>((
                        table.qualified_name(),
                        dat_file,
                        wrote_data,
                    ))
                });

                join_handles.push((idx, handle));
            }

            // Collect results in order (by idx) so the TOC is deterministic.
            let mut results: Vec<(usize, String, String, bool)> = Vec::new();
            for (idx, handle) in join_handles {
                let (qname, dat_file, wrote) = handle
                    .await
                    .context("parallel task panicked")?
                    .context("parallel task failed")?;
                results.push((idx, qname, dat_file, wrote));
            }
            results.sort_by_key(|(idx, _, _, _)| *idx);

            for (_idx, qname, dat_file, wrote) in results {
                if wrote {
                    toc.push_str(&format!("DATA {} {dat_file}\n", qname));
                }
            }
        }
    }

    // Write toc.dat.
    let toc_path = dir_path.join("toc.dat");
    std::fs::write(&toc_path, &toc).context("failed to write toc.dat")?;

    Ok(())
}

/// Return all enum types in the database (schema, name, DDL).
async fn get_enum_types(client: &Client) -> Result<Vec<(String, String, String)>> {
    let rows = client
        .query(
            "SELECT n.nspname AS type_schema,
                    t.typname AS type_name,
                    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
             FROM pg_catalog.pg_type t
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
             JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
             WHERE t.typtype = 'e'
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
             GROUP BY n.nspname, t.typname
             ORDER BY n.nspname, t.typname",
            &[],
        )
        .await
        .context("failed to query enum types")?;

    let mut result = Vec::new();
    for row in &rows {
        let type_schema: &str = row.get("type_schema");
        let type_name: &str = row.get("type_name");
        let labels: Vec<String> = row.get("labels");
        let label_list = labels
            .iter()
            .map(|l| format!("'{}'", l.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let qname = format!(
            "{}.{}",
            catalog::quote_ident(type_schema),
            catalog::quote_ident(type_name)
        );
        let ddl = format!("CREATE TYPE {qname} AS ENUM ({label_list});\n");
        result.push((type_schema.to_string(), type_name.to_string(), ddl));
    }
    Ok(result)
}

/// Return sequences that any column of this table depends on (via DEFAULT nextval).
///
/// Returns a list of (schema, name, create_ddl) tuples.
async fn get_table_sequences(
    client: &Client,
    table: &catalog::TableInfo,
) -> Result<Vec<(String, String, String)>> {
    let rows = client
        .query(
            "SELECT DISTINCT n.nspname AS seq_schema, s.relname AS seq_name
             FROM pg_catalog.pg_class t
             JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
             JOIN pg_catalog.pg_depend d ON d.refobjid = t.oid
             JOIN pg_catalog.pg_class s ON s.oid = d.objid
             JOIN pg_catalog.pg_namespace n ON n.oid = s.relnamespace
             WHERE t.relkind = 'r'
               AND s.relkind = 'S'
               AND tn.nspname = $1
               AND t.relname = $2
               AND d.deptype = 'a'",
            &[&table.schema, &table.name],
        )
        .await
        .context("failed to query sequences for table")?;

    let mut result = Vec::new();
    for row in &rows {
        let seq_schema: &str = row.get("seq_schema");
        let seq_name: &str = row.get("seq_name");
        let ddl = get_sequence_ddl(client, seq_schema, seq_name).await?;
        result.push((seq_schema.to_string(), seq_name.to_string(), ddl));
    }
    Ok(result)
}

/// Generate CREATE SEQUENCE DDL for a sequence.
async fn get_sequence_ddl(client: &Client, schema: &str, name: &str) -> Result<String> {
    let rows = client
        .query(
            "SELECT s.start_value, s.minimum_value, s.maximum_value,
                    s.increment, s.cycle_option
             FROM information_schema.sequences s
             WHERE s.sequence_schema = $1 AND s.sequence_name = $2",
            &[&schema, &name],
        )
        .await
        .context("failed to query sequence definition")?;

    if let Some(row) = rows.first() {
        let start: &str = row.get("start_value");
        let min: &str = row.get("minimum_value");
        let max: &str = row.get("maximum_value");
        let inc: &str = row.get("increment");
        let cycle: &str = row.get("cycle_option");

        let qname = format!(
            "{}.{}",
            catalog::quote_ident(schema),
            catalog::quote_ident(name)
        );
        let cycle_clause = if cycle == "YES" { " CYCLE" } else { "" };

        Ok(format!(
            "CREATE SEQUENCE {qname}\n    INCREMENT BY {inc}\n    MINVALUE {min}\n    MAXVALUE {max}\n    START WITH {start}{cycle_clause};\n"
        ))
    } else {
        // Fallback: minimal sequence definition.
        let qname = format!(
            "{}.{}",
            catalog::quote_ident(schema),
            catalog::quote_ident(name)
        );
        Ok(format!("CREATE SEQUENCE {qname};\n"))
    }
}
