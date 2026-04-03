// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_plumbing — pg_dump/pg_restore rewritten in Rust.

use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use pg_plumbing::{dump, restore};

/// pg_dump/pg_restore rewritten in Rust.
#[derive(Parser, Debug)]
#[command(name = "pg_plumbing", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Dump a PostgreSQL database into a script file or archive.
    PgDump(PgDumpArgs),
    /// Restore a PostgreSQL database from an archive created by pg_dump.
    PgRestore(PgRestoreArgs),
}

/// Arguments for the pg_dump subcommand.
#[derive(Parser, Debug)]
pub struct PgDumpArgs {
    /// Output format: plain, custom, directory, tar.
    #[arg(short = 'F', long = "format", default_value = "plain")]
    format: DumpFormat,

    /// Output file or directory name.
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// Dump only the named table(s).
    #[arg(short = 't', long = "table")]
    table: Vec<String>,

    /// Database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Dump only the schema (no data).
    #[arg(short = 's', long = "schema-only")]
    schema_only: bool,

    /// Dump only the data (no schema).
    #[arg(short = 'a', long = "data-only")]
    data_only: bool,

    /// Use INSERT statements instead of COPY.
    #[arg(long = "inserts")]
    inserts: bool,

    /// Use INSERT statements with column names.
    #[arg(long = "column-inserts")]
    column_inserts: bool,

    /// Dump data as INSERT with multiple rows per statement.
    #[arg(long = "rows-per-insert")]
    rows_per_insert: Option<u32>,

    /// Dump only the named schema(s).
    #[arg(short = 'n', long = "schema")]
    schema: Vec<String>,

    /// Do not dump the named schema(s).
    #[arg(short = 'N', long = "exclude-schema")]
    exclude_schema: Vec<String>,

    /// Do not dump the named table(s).
    #[arg(short = 'T', long = "exclude-table")]
    exclude_table: Vec<String>,

    /// Suppress output of ownership changes.
    #[arg(long = "no-owner")]
    no_owner: bool,

    /// Suppress output of access privileges.
    #[arg(long = "no-privileges", alias = "no-acl")]
    no_privileges: bool,

    /// Positional database name (alternative to -d).
    #[arg()]
    database: Option<String>,
}

/// Output format for pg_dump.
#[derive(Debug, Clone, ValueEnum)]
pub enum DumpFormat {
    /// Plain SQL script.
    #[value(alias = "p")]
    Plain,
    /// Custom archive.
    #[value(alias = "c")]
    Custom,
    /// Directory archive.
    #[value(alias = "d")]
    Directory,
    /// Tar archive.
    #[value(alias = "t")]
    Tar,
}

/// Arguments for the pg-restore subcommand.
#[derive(Parser, Debug)]
pub struct PgRestoreArgs {
    /// Target database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Drop database objects before recreating them.
    #[arg(short = 'c', long = "clean")]
    clean: bool,

    /// Input archive file (positional).
    filename: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::PgDump(args) => run_pg_dump(args).await,
        Command::PgRestore(args) => run_pg_restore(args).await,
    }
}

async fn run_pg_dump(args: PgDumpArgs) -> Result<()> {
    let dbname = args
        .dbname
        .as_deref()
        .or(args.database.as_deref())
        .unwrap_or("postgres");

    let opts = dump::DumpOptions {
        dbname: dbname.to_string(),
        tables: args.table,
        schema_only: args.schema_only,
        data_only: args.data_only,
        inserts: args.inserts || args.column_inserts || args.rows_per_insert.is_some(),
        column_inserts: args.column_inserts,
        rows_per_insert: args.rows_per_insert,
        schemas: args.schema,
        exclude_schemas: args.exclude_schema,
        exclude_tables: args.exclude_table,
        no_owner: args.no_owner,
        no_privileges: args.no_privileges,
    };

    let output = dump::dump_plain(&opts).await?;

    match args.file {
        Some(ref path) => std::fs::write(path, &output)?,
        None => print!("{output}"),
    }

    Ok(())
}

async fn run_pg_restore(args: PgRestoreArgs) -> Result<()> {
    let dbname = match args.dbname {
        Some(ref d) => d.clone(),
        None => bail!("pg_restore: no database specified (use -d)"),
    };

    let filename = match args.filename {
        Some(ref f) => f.clone(),
        None => bail!("pg_restore: no input file specified"),
    };

    let sql = if filename == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    } else {
        std::fs::read_to_string(&filename)?
    };

    let opts = restore::RestoreOptions {
        dbname,
        clean: args.clean,
    };

    restore::restore_plain(&sql, &opts).await
}
