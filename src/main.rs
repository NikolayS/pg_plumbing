// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

use anyhow::Result;
use clap::{Parser, Subcommand};

/// pg_dump/pg_restore rewritten in Rust.
#[derive(Parser)]
#[command(name = "pg_plumbing", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Top-level subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Dump a PostgreSQL database.
    #[command(version)]
    PgDump(PgDumpArgs),
    /// Restore a PostgreSQL database from an archive.
    #[command(version)]
    PgRestore(PgRestoreArgs),
}

/// Arguments for the pg_dump subcommand.
#[derive(Parser)]
struct PgDumpArgs {
    /// Output format: plain, custom, directory, tar.
    #[arg(short = 'F', long = "format")]
    format: Option<String>,

    /// Output file or directory.
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// Database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Dump only matching table(s).
    #[arg(short = 't', long = "table")]
    table: Option<Vec<String>>,

    /// Exclude matching table(s).
    #[arg(short = 'T', long = "exclude-table")]
    exclude_table: Option<Vec<String>>,

    /// Dump only matching schema(s).
    #[arg(short = 'n', long = "schema")]
    schema: Option<Vec<String>>,

    /// Exclude matching schema(s).
    #[arg(short = 'N', long = "exclude-schema")]
    exclude_schema: Option<Vec<String>>,

    /// Dump only schema, no data.
    #[arg(short = 's', long = "schema-only")]
    schema_only: bool,

    /// Dump only data, no schema.
    #[arg(short = 'a', long = "data-only")]
    data_only: bool,

    /// Number of parallel jobs.
    #[arg(short = 'j', long = "jobs")]
    jobs: Option<usize>,

    /// Do not output commands to set ownership.
    #[arg(long = "no-owner")]
    no_owner: bool,

    /// Do not output privilege commands.
    #[arg(long = "no-privileges")]
    no_privileges: bool,

    /// Use IF EXISTS when dropping objects.
    #[arg(long = "if-exists")]
    if_exists: bool,

    /// Output commands to clean (drop) objects before creating.
    #[arg(long = "clean")]
    clean: bool,

    /// Include commands to create the database.
    #[arg(long = "create")]
    create: bool,

    /// Compression level.
    #[arg(short = 'Z', long = "compress")]
    compress: Option<String>,
}

/// Arguments for the pg_restore subcommand.
#[derive(Parser)]
struct PgRestoreArgs {
    /// Database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Input format: custom, directory, tar.
    #[arg(short = 'F', long = "format")]
    format: Option<String>,

    /// Number of parallel jobs.
    #[arg(short = 'j', long = "jobs")]
    jobs: Option<usize>,

    /// Restore only matching table(s).
    #[arg(short = 't', long = "table")]
    table: Option<Vec<String>>,

    /// Restore only matching schema(s).
    #[arg(short = 'n', long = "schema")]
    schema: Option<Vec<String>>,

    /// Restore only schema, no data.
    #[arg(short = 's', long = "schema-only")]
    schema_only: bool,

    /// Restore only data, no schema.
    #[arg(short = 'a', long = "data-only")]
    data_only: bool,

    /// Do not output commands to set ownership.
    #[arg(long = "no-owner")]
    no_owner: bool,

    /// Output commands to clean (drop) objects before restoring.
    #[arg(long = "clean")]
    clean: bool,

    /// Use IF EXISTS when dropping objects.
    #[arg(long = "if-exists")]
    if_exists: bool,

    /// Include commands to create the database.
    #[arg(long = "create")]
    create: bool,

    /// List the table of contents.
    #[arg(short = 'l', long = "list")]
    list: bool,

    /// Restore using a custom TOC list file.
    #[arg(short = 'L', long = "use-list")]
    use_list: Option<String>,

    /// Input file (archive).
    input_file: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::PgDump(_args) => {
            eprintln!("pg_dump: not yet implemented");
        }
        Commands::PgRestore(_args) => {
            eprintln!("pg_restore: not yet implemented");
        }
    }

    Ok(())
}
