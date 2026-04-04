// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_restore — restore a PostgreSQL database from an archive.

use clap::Parser;
use pg_plumbing::restore;
use pg_plumbing::ConnParams;

/// pg_restore restores a PostgreSQL database from an archive
/// created by pg_dump.
///
/// Compatible with PostgreSQL's pg_restore.
#[derive(Parser)]
#[command(
    name = "pg_restore",
    version = pg_restore_version(),
    about = "pg_restore restores a PostgreSQL database from an archive created by pg_dump.",
    // Disable clap's automatic -h/--help short flag so that -h can be
    // used for --host (matching PostgreSQL's pg_restore interface).
    // --help still works via the long flag.
    disable_help_flag = true,
)]
struct Cli {
    /// Print help information
    #[arg(long = "help", action = clap::ArgAction::Help)]
    help: (),

    /// Target database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Output file/script (use - for stdout)
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// Drop database objects before recreating them.
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

    /// Number of parallel jobs
    #[arg(short = 'j', long = "jobs", allow_negative_numbers = true)]
    jobs: Option<String>,

    /// Restore in a single transaction
    #[arg(short = '1', long = "single-transaction")]
    single_transaction: bool,

    /// Archive format
    #[arg(short = 'F', long = "format")]
    format: Option<String>,

    /// Create the target database
    #[arg(short = 'C', long = "create")]
    create: bool,

    /// Exclude databases matching pattern (dumpall only)
    #[arg(long = "exclude-database")]
    exclude_database: Option<String>,

    /// Restore only global objects
    #[arg(short = 'g', long = "globals-only")]
    globals_only: bool,

    /// Do not restore global objects
    #[arg(long = "no-globals")]
    no_globals: bool,

    /// Archive file(s) to restore (positional).
    #[arg()]
    filenames: Vec<String>,

    // ---- Connection options ----
    /// Database server host or socket directory (overrides PGHOST)
    #[arg(short = 'h', long = "host")]
    host: Option<String>,

    /// Database server port (overrides PGPORT)
    #[arg(short = 'p', long = "port")]
    port: Option<String>,

    /// Connect as the specified database user (overrides PGUSER)
    #[arg(short = 'U', long = "username")]
    username: Option<String>,

    /// Force password prompt (password may also be supplied via PGPASSWORD)
    #[arg(short = 'W', long = "password")]
    password: Option<String>,
}

/// Build the version string: `pg_restore (pg_plumbing) <version>`.
fn pg_restore_version() -> &'static str {
    concat!("pg_restore (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

fn main() {
    let cli = Cli::parse();

    // Too many positional args (more than 1 file)
    if cli.filenames.len() > 1 {
        eprintln!(
            "pg_restore: error: too many command-line arguments (first is \"{}\")",
            cli.filenames[1]
        );
        std::process::exit(1);
    }

    // Validate format if provided
    if let Some(ref fmt) = cli.format {
        if fmt.is_empty() {
            eprintln!("pg_restore: error: unrecognized archive format \"\";");
            std::process::exit(1);
        }
        if !validate_format(fmt) {
            eprintln!("pg_restore: error: unrecognized archive format \"{fmt}\";");
            std::process::exit(1);
        }
    }

    // --data-only + --schema-only
    if cli.data_only && cli.schema_only {
        eprintln!(
            "pg_restore: error: options -s/--schema-only and -a/--data-only cannot be used together"
        );
        std::process::exit(1);
    }

    // -d and -f cannot be used together
    if cli.dbname.is_some() && cli.file.is_some() {
        eprintln!("pg_restore: error: options -d/--dbname and -f/--file cannot be used together");
        std::process::exit(1);
    }

    // --clean + --data-only
    if cli.clean && cli.data_only {
        eprintln!(
            "pg_restore: error: options -c/--clean and -a/--data-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --if-exists requires --clean
    if cli.if_exists && !cli.clean {
        eprintln!("pg_restore: error: option --if-exists requires option -c/--clean");
        std::process::exit(1);
    }

    // Validate jobs
    if let Some(ref jobs_str) = cli.jobs {
        match jobs_str.parse::<i64>() {
            Ok(n) if !(1..=1000).contains(&n) => {
                eprintln!("pg_restore: error: -j/--jobs must be in range");
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!("pg_restore: error: -j/--jobs must be in range \"{jobs_str}\"");
                std::process::exit(1);
            }
            Ok(_) => {}
        }
    }

    // --single-transaction + -j
    if cli.single_transaction && cli.jobs.is_some() {
        eprintln!("pg_restore: error: cannot specify both --single-transaction and multiple jobs");
        std::process::exit(1);
    }

    // --create + --single-transaction
    if cli.create && cli.single_transaction {
        eprintln!(
            "pg_restore: error: options -C/--create and -1/--single-transaction cannot be used together"
        );
        std::process::exit(1);
    }

    // --exclude-database + --globals-only
    if cli.exclude_database.is_some() && cli.globals_only {
        eprintln!(
            "pg_restore: options --exclude-database and --globals-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --data-only + --globals-only
    if cli.data_only && cli.globals_only {
        eprintln!(
            "pg_restore: error: options -a/--data-only and --globals-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only + --schema-only
    if cli.globals_only && cli.schema_only {
        eprintln!(
            "pg_restore: options --globals-only and -s/--schema-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only + --statistics-only
    if cli.globals_only && cli.statistics_only {
        eprintln!(
            "pg_restore: options --globals-only and --statistics-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only + --no-globals
    if cli.globals_only && cli.no_globals {
        eprintln!(
            "pg_restore: error: options --globals-only and --no-globals cannot be used together"
        );
        std::process::exit(1);
    }

    // --globals-only requires dumpall archive.
    // Since we don't implement actual archive inspection yet, we always emit
    // this error when --globals-only is used with a positional file
    // (which would need to be a real dumpall archive to proceed).
    if cli.globals_only && !cli.filenames.is_empty() {
        eprintln!("pg_restore: error: --globals-only can only be used with pg_dumpall archives");
        std::process::exit(1);
    }

    // --exclude-database requires dumpall archive
    if cli.exclude_database.is_some() && !cli.filenames.is_empty() {
        eprintln!(
            "pg_restore: error: --exclude-database can only be used with pg_dumpall archives"
        );
        std::process::exit(1);
    }

    // Require either -d or -f or a positional file
    let has_output = cli.dbname.is_some() || cli.file.is_some() || !cli.filenames.is_empty();
    if !has_output {
        eprintln!("pg_restore: error: one of -d/--dbname and -f/--file must be specified");
        std::process::exit(1);
    }

    // Require a database target.
    let raw_dbname = match cli.dbname {
        Some(ref d) => d.clone(),
        None => {
            eprintln!("pg_restore: error: no database specified (use -d)");
            std::process::exit(1);
        }
    };

    // Build connection params from CLI flags (they override env vars inside
    // build_conninfo_with_params).
    let conn_params = ConnParams {
        host: cli.host.clone(),
        port: cli.port.clone(),
        user: cli.username.clone(),
        password: cli.password.clone(),
    };
    let conninfo = pg_plumbing::build_conninfo_with_params(&raw_dbname, &conn_params);

    // Require a positional file.
    let filename = match cli.filenames.first() {
        Some(f) => f.clone(),
        None => {
            eprintln!("pg_restore: error: no input file specified");
            std::process::exit(1);
        }
    };

    let jobs: usize = cli
        .jobs
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);

    let opts = restore::RestoreOptions {
        dbname: raw_dbname.clone(),
        conninfo,
        clean: cli.clean,
        if_exists: cli.if_exists,
        jobs,
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Directory format: detect before trying to read as a file.
    if std::path::Path::new(&filename).is_dir() {
        rt.block_on(restore::restore_directory(&filename, &opts))
            .unwrap_or_else(|e| {
                eprintln!("pg_restore: error: {e}");
                std::process::exit(1);
            });
        return;
    }

    let file_bytes = std::fs::read(&filename).unwrap_or_else(|e| {
        eprintln!("pg_restore: error: could not open file \"{filename}\": {e}");
        std::process::exit(1);
    });

    if restore::is_custom_format(&file_bytes) {
        rt.block_on(restore::restore_custom(&file_bytes, &opts))
            .unwrap_or_else(|e| {
                eprintln!("pg_restore: error: {e}");
                std::process::exit(1);
            });
    } else {
        // Assume plain SQL format.
        let sql = String::from_utf8_lossy(&file_bytes).to_string();
        rt.block_on(restore::restore_plain(&sql, &opts))
            .unwrap_or_else(|e| {
                eprintln!("pg_restore: error: {e}");
                std::process::exit(1);
            });
    }
}

fn validate_format(fmt: &str) -> bool {
    matches!(
        fmt,
        "plain" | "p" | "custom" | "c" | "directory" | "d" | "tar" | "t"
    )
}
