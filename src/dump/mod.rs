// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_dump implementation.

pub mod catalog;
pub mod directory_format;
pub mod filter;
pub mod format;

use anyhow::{Context, Result};
use tokio_postgres::NoTls;

/// Options controlling what and how to dump.
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Database name or connection string.
    pub dbname: String,
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
    /// Suppress ownership statements.
    pub no_owner: bool,
    /// Suppress privilege statements.
    pub no_privileges: bool,
}

/// Dump a database in plain SQL format.
pub async fn dump_plain(opts: &DumpOptions) -> Result<String> {
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

    let mut out = String::new();

    // Header
    out.push_str("--\n");
    out.push_str("-- PostgreSQL database dump\n");
    out.push_str("--\n\n");

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

    for table in &tables {
        if !opts.data_only {
            format::write_create_table(&mut out, table);
            out.push('\n');
        }

        if !opts.schema_only {
            format::write_table_data(&mut out, &client, table, opts).await?;
            out.push('\n');
        }
    }

    out.push_str("--\n");
    out.push_str("-- PostgreSQL database dump complete\n");
    out.push_str("--\n\n");

    Ok(out)
}
