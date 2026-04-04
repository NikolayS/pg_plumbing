// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Directory format dump (`pg_dump -F directory`).
//!
//! Creates a directory containing:
//!   - `toc.dat` — text TOC listing schema DDL and data file references
//!   - `<N>.dat`  — one plain COPY-format file per table with data

use anyhow::{bail, Context, Result};
use tokio_postgres::{Client, NoTls};

use super::catalog;
use super::format;
use super::DumpOptions;

/// Dump a database in directory format.
///
/// Creates `output_dir` (errors if it already exists and is non-empty),
/// writes `toc.dat` and one `<N>.dat` per table.
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

    // Connect to the database.
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

    // Schema section.
    if !opts.data_only {
        for table in &tables {
            // Dump any sequences that this table's columns depend on first.
            let sequences = get_table_sequences(&client, table).await?;
            for (seq_schema, seq_name, seq_ddl) in &sequences {
                let seq_key = format!("{seq_schema}.{seq_name}");
                let seq_file = format!("{seq_name}.seq.ddl");
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

    // Data section.
    if !opts.schema_only {
        for (idx, table) in tables.iter().enumerate() {
            let dat_file = format!("{}.dat", idx + 1);
            let dat_path = dir_path.join(&dat_file);

            let mut data_buf = String::new();
            format::write_table_data(&mut data_buf, &client, table, opts).await?;

            // Only write data file if there is actual data.
            if !data_buf.is_empty() {
                std::fs::write(&dat_path, &data_buf)
                    .with_context(|| format!("failed to write {dat_file}"))?;
                toc.push_str(&format!("DATA {} {dat_file}\n", table.qualified_name()));
            }
        }
    }

    // Write toc.dat.
    let toc_path = dir_path.join("toc.dat");
    std::fs::write(&toc_path, &toc).context("failed to write toc.dat")?;

    Ok(())
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
