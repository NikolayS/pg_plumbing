// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_dumpall — dump all PostgreSQL databases.

use clap::Parser;

/// pg_dumpall extracts a PostgreSQL database cluster into an SQL script file.
///
/// Compatible with PostgreSQL's pg_dumpall.
#[derive(Parser)]
#[command(
    name = "pg_dumpall",
    version = pg_dumpall_version(),
    about = "pg_dumpall extracts a PostgreSQL database cluster into an SQL script file."
)]
struct Cli {
    /// Positional arguments (should be empty)
    #[arg()]
    extra_args: Vec<String>,

    /// Drop database objects before recreating them
    #[arg(short = 'c', long = "clean")]
    clean: bool,

    /// Use DROP ... IF EXISTS
    #[arg(long = "if-exists")]
    if_exists: bool,

    /// Dump only the data (no schema)
    #[arg(short = 'a', long = "data-only")]
    data_only: bool,

    /// Dump only the schema (no data)
    #[arg(short = 's', long = "schema-only")]
    schema_only: bool,

    /// Dump only statistics
    #[arg(long = "statistics-only")]
    statistics_only: bool,

    /// Do not dump statistics
    #[arg(long = "no-statistics")]
    no_statistics: bool,

    /// Dump only global objects
    #[arg(short = 'g', long = "globals-only")]
    globals_only: bool,

    /// Dump only roles
    #[arg(short = 'r', long = "roles-only")]
    roles_only: bool,

    /// Dump only tablespaces
    #[arg(short = 't', long = "tablespaces-only")]
    tablespaces_only: bool,

    /// Exclude database matching pattern
    #[arg(long = "exclude-database")]
    exclude_database: Option<String>,

    /// Output format: plain (p), custom (c), directory (d), tar (t)
    #[arg(long = "format")]
    format: Option<String>,

    /// Output file or directory
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// Do not dump data
    #[arg(long = "no-data")]
    no_data: bool,

    /// Do not dump schema
    #[arg(long = "no-schema")]
    no_schema: bool,

    /// Include statistics in dump
    #[arg(long = "statistics")]
    statistics: bool,

    /// Restrict key for dump (only with --format=plain)
    #[arg(long = "restrict-key")]
    restrict_key: Option<String>,
}

/// Build the version string: `pg_dumpall (pg_plumbing) <version>`.
fn pg_dumpall_version() -> &'static str {
    concat!("pg_dumpall (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

fn validate_format(fmt: &str) -> bool {
    matches!(
        fmt,
        "plain" | "p" | "custom" | "c" | "directory" | "d" | "tar" | "t"
    )
}

fn is_plain_format(fmt: &str) -> bool {
    matches!(fmt, "plain" | "p")
}

fn main() {
    let cli = Cli::parse();

    // Too many positional args
    if cli.extra_args.len() > 1 {
        eprintln!(
            "pg_dumpall: error: too many command-line arguments (first is \"{}\")",
            cli.extra_args[0]
        );
        std::process::exit(1);
    }

    // Validate format if provided
    if let Some(ref fmt) = cli.format {
        if !validate_format(fmt) {
            eprintln!("pg_dumpall: error: invalid output format \"{fmt}\"");
            std::process::exit(1);
        }
    }

    let format_str = cli.format.as_deref().unwrap_or("plain");

    // --clean + --data-only
    if cli.clean && cli.data_only {
        eprintln!(
            "pg_dumpall: error: options -c/--clean and -a/--data-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only + --roles-only
    if cli.globals_only && cli.roles_only {
        eprintln!(
            "pg_dumpall: error: options -g/--globals-only and -r/--roles-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only + --tablespaces-only
    if cli.globals_only && cli.tablespaces_only {
        eprintln!(
            "pg_dumpall: error: options -g/--globals-only and -t/--tablespaces-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --roles-only + --tablespaces-only
    if cli.roles_only && cli.tablespaces_only {
        eprintln!(
            "pg_dumpall: error: options -r/--roles-only and -t/--tablespaces-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --if-exists requires --clean
    if cli.if_exists && !cli.clean {
        eprintln!("pg_dumpall: error: option --if-exists requires option -c/--clean");
        std::process::exit(1);
    }

    // --exclude-database + --globals-only
    if cli.exclude_database.is_some() && cli.globals_only {
        eprintln!(
            "pg_dumpall: error: option --exclude-database cannot be used together with -g/--globals-only"
        );
        std::process::exit(1);
    }

    // --data-only + --no-data
    if cli.data_only && cli.no_data {
        eprintln!(
            "pg_dumpall: error: options -a/--data-only and --no-data cannot be used together"
        );
        std::process::exit(1);
    }

    // --schema-only + --no-schema
    if cli.schema_only && cli.no_schema {
        eprintln!(
            "pg_dumpall: error: options -s/--schema-only and --no-schema cannot be used together"
        );
        std::process::exit(1);
    }

    // --statistics-only + --no-statistics
    if cli.statistics_only && cli.no_statistics {
        eprintln!(
            "pg_dumpall: options --statistics-only and --no-statistics cannot be used together"
        );
        std::process::exit(1);
    }

    // --statistics + --no-statistics
    if cli.statistics && cli.no_statistics {
        eprintln!(
            "pg_dumpall: error: options --statistics and --no-statistics cannot be used together"
        );
        std::process::exit(1);
    }

    // --statistics + --tablespaces-only
    if cli.statistics && cli.tablespaces_only {
        eprintln!(
            "pg_dumpall: options --statistics and -t/--tablespaces-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --restrict-key can only be used with --format=plain
    if cli.restrict_key.is_some() && !is_plain_format(format_str) {
        eprintln!("pg_dumpall: error: option --restrict-key can only be used with --format=plain");
        std::process::exit(1);
    }

    // --clean + --globals-only in non-plain format
    if cli.clean && cli.globals_only && !is_plain_format(format_str) {
        eprintln!(
            "pg_dumpall: options -c/--clean and --globals-only cannot be used together in non-plain format"
        );
        std::process::exit(1);
    }

    // Non-plain format requires --file
    if !is_plain_format(format_str) && cli.file.is_none() {
        eprintln!("pg_dumpall: error: non-plain output format requires -f/--file option");
        std::process::exit(1);
    }

    // Actual dump not yet implemented.
    eprintln!("pg_dumpall: error: not yet implemented");
    std::process::exit(1);
}
