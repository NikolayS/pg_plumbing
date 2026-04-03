// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_restore implementation.

pub mod parallel;
pub mod parse;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::SinkExt;
use tokio_postgres::NoTls;

/// Options controlling how to restore.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// Target database name or connection string.
    pub dbname: String,
    /// Drop objects before recreating them.
    pub clean: bool,
}

/// A parsed segment of a SQL dump file.
#[derive(Debug)]
enum SqlSegment {
    /// One or more regular SQL statements.
    Statements(String),
    /// A COPY ... FROM stdin block with its data.
    CopyBlock {
        /// The COPY command line (e.g. `COPY public.t (a, b) FROM stdin;`).
        header: String,
        /// The tab-separated data lines (without the terminating `\.`).
        data: String,
    },
}

/// Restore a directory-format dump to a database.
///
/// Reads `toc.dat` from `input_dir`, executes schema DDL files listed
/// therein, then streams each data `.dat` file via COPY.
pub async fn restore_directory(input_dir: &str, opts: &RestoreOptions) -> Result<()> {
    let dir_path = std::path::Path::new(input_dir);
    if !dir_path.is_dir() {
        anyhow::bail!("\"{}\" is not a directory", input_dir);
    }

    let toc_path = dir_path.join("toc.dat");
    let toc = std::fs::read_to_string(&toc_path)
        .with_context(|| format!("failed to read toc.dat in \"{}\"", input_dir))?;

    // Parse TOC lines into (kind, qualified_name, filename) entries.
    // Format: `TABLE <qname> <file>` or `DATA <qname> <file>` (comments start with `;`)
    let mut schema_files: Vec<String> = Vec::new();
    let mut data_entries: Vec<(String, String)> = Vec::new(); // (qname, file)

    for line in toc.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with(';') {
            continue;
        }
        let mut parts = line.splitn(3, ' ');
        let kind = parts.next().unwrap_or("");
        let _qname = parts.next().unwrap_or("").to_string();
        let file = parts.next().unwrap_or("").to_string();

        match kind {
            "SEQUENCE" | "TABLE" => {
                schema_files.push(file);
            }
            "DATA" => {
                data_entries.push((_qname, file));
            }
            _ => {} // ignore unknown entries
        }
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

    // Execute schema DDL files.
    for ddl_file in &schema_files {
        let ddl_path = dir_path.join(ddl_file);
        let ddl = std::fs::read_to_string(&ddl_path)
            .with_context(|| format!("failed to read DDL file {ddl_file}"))?;
        let trimmed = ddl.trim();
        if !trimmed.is_empty() {
            client
                .batch_execute(trimmed)
                .await
                .with_context(|| format!("failed to execute DDL from {ddl_file}"))?;
        }
    }

    // Restore data files via COPY.
    for (qname, dat_file) in &data_entries {
        let dat_path = dir_path.join(dat_file);
        let dat = std::fs::read_to_string(&dat_path)
            .with_context(|| format!("failed to read data file {dat_file}"))?;

        // Parse the COPY block from the .dat file.
        let segments = parse_sql_segments(&dat);
        for segment in &segments {
            match segment {
                SqlSegment::Statements(stmts) => {
                    let trimmed = stmts.trim();
                    if !trimmed.is_empty() {
                        client
                            .batch_execute(trimmed)
                            .await
                            .with_context(|| format!("failed to execute SQL from {dat_file}"))?;
                    }
                }
                SqlSegment::CopyBlock { header, data } => {
                    let sink = client
                        .copy_in(header.as_str())
                        .await
                        .with_context(|| format!("failed to start COPY for {qname}"))?;
                    let mut sink = Box::pin(sink);
                    let data_bytes = bytes::Bytes::from(data.clone());
                    futures_util::SinkExt::send(&mut sink, data_bytes)
                        .await
                        .context("failed to send COPY data")?;
                    futures_util::SinkExt::close(&mut sink)
                        .await
                        .context("failed to finish COPY")?;
                }
            }
        }
    }

    Ok(())
}

/// Restore a plain-format SQL dump to a database.
pub async fn restore_plain(sql: &str, opts: &RestoreOptions) -> Result<()> {
    let conninfo = crate::build_conninfo(&opts.dbname);
    let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .with_context(|| format!("failed to connect to database \"{}\"", opts.dbname))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    if opts.clean {
        let drop_stmts = generate_drop_statements(sql);
        if !drop_stmts.is_empty() {
            client
                .batch_execute(&drop_stmts)
                .await
                .context("failed to execute clean (DROP) statements")?;
        }
    }

    let segments = parse_sql_segments(sql);

    for segment in &segments {
        match segment {
            SqlSegment::Statements(stmts) => {
                let trimmed = stmts.trim();
                if !trimmed.is_empty() {
                    client.batch_execute(trimmed).await.with_context(|| {
                        let preview: String = trimmed.chars().take(100).collect();
                        format!("failed to execute SQL: {preview}")
                    })?;
                }
            }
            SqlSegment::CopyBlock { header, data } => {
                let sink = client
                    .copy_in(header.as_str())
                    .await
                    .with_context(|| format!("failed to start COPY: {header}"))?;
                let mut sink = Box::pin(sink);
                let data_bytes = Bytes::from(data.clone());
                sink.send(data_bytes)
                    .await
                    .context("failed to send COPY data")?;
                sink.close().await.context("failed to finish COPY")?;
            }
        }
    }

    Ok(())
}

/// Parse a plain-format SQL dump into executable segments.
///
/// Splits the input into regular SQL statement blocks and COPY FROM stdin
/// blocks. Comment-only lines and blank lines are preserved in the
/// statement blocks so that `batch_execute` can handle them.
fn parse_sql_segments(sql: &str) -> Vec<SqlSegment> {
    let mut segments = Vec::new();
    let mut current_sql = String::new();
    let mut in_copy = false;
    let mut copy_header = String::new();
    let mut copy_data = String::new();

    for line in sql.lines() {
        if in_copy {
            if line == "\\." {
                // End of COPY data block.
                segments.push(SqlSegment::CopyBlock {
                    header: copy_header.clone(),
                    data: copy_data.clone(),
                });
                copy_header.clear();
                copy_data.clear();
                in_copy = false;
            } else {
                copy_data.push_str(line);
                copy_data.push('\n');
            }
        } else if line.starts_with("COPY ") && line.contains("FROM stdin") && line.ends_with(';') {
            // Flush any accumulated SQL.
            if !current_sql.trim().is_empty() {
                segments.push(SqlSegment::Statements(current_sql.clone()));
                current_sql.clear();
            }
            copy_header = line.to_string();
            in_copy = true;
        } else {
            current_sql.push_str(line);
            current_sql.push('\n');
        }
    }

    // Flush remaining SQL.
    if !current_sql.trim().is_empty() {
        segments.push(SqlSegment::Statements(current_sql));
    }

    segments
}

/// Generate DROP IF EXISTS statements for objects found in the dump SQL.
///
/// Scans for `CREATE TABLE` and `CREATE SEQUENCE` statements and
/// produces corresponding `DROP ... IF EXISTS ... CASCADE;` statements.
fn generate_drop_statements(sql: &str) -> String {
    let mut drops = String::new();

    for line in sql.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("CREATE TABLE ") {
            if let Some(name) = rest.split(&['(', ' '][..]).next() {
                drops.push_str(&format!("DROP TABLE IF EXISTS {name} CASCADE;\n"));
            }
        } else if let Some(rest) = trimmed.strip_prefix("CREATE SEQUENCE ") {
            if let Some(name) = rest.split(&[' ', ';'][..]).next() {
                drops.push_str(&format!("DROP SEQUENCE IF EXISTS {name} CASCADE;\n"));
            }
        }
    }

    drops
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_segments_basic() {
        let sql = "\
SET statement_timeout = 0;

CREATE TABLE public.t (id integer);

COPY public.t (id) FROM stdin;
1
2
3
\\.

SELECT 1;
";
        let segments = parse_sql_segments(sql);
        assert_eq!(segments.len(), 3);
        assert!(matches!(&segments[0], SqlSegment::Statements(_)));
        assert!(matches!(&segments[1], SqlSegment::CopyBlock { .. }));
        assert!(matches!(&segments[2], SqlSegment::Statements(_)));

        if let SqlSegment::CopyBlock { header, data } = &segments[1] {
            assert_eq!(header, "COPY public.t (id) FROM stdin;");
            assert_eq!(data, "1\n2\n3\n");
        }
    }

    #[test]
    fn generate_drops_for_tables() {
        let sql = "\
CREATE TABLE public.foo (id integer);
CREATE TABLE public.bar (name text);
CREATE SEQUENCE public.foo_id_seq;
";
        let drops = generate_drop_statements(sql);
        assert!(drops.contains("DROP TABLE IF EXISTS public.foo CASCADE;"));
        assert!(drops.contains("DROP TABLE IF EXISTS public.bar CASCADE;"));
        assert!(drops.contains("DROP SEQUENCE IF EXISTS public.foo_id_seq CASCADE;"));
    }

    #[test]
    fn generate_drops_empty_for_comments() {
        let sql = "-- just a comment\nSET x = 1;\n";
        let drops = generate_drop_statements(sql);
        assert!(drops.is_empty());
    }
}
