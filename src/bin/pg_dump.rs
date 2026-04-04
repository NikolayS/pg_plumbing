// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_dump — dump a PostgreSQL database.

use clap::Parser;
use pg_plumbing::dump;

/// pg_dump dumps a database as a text file or to other formats.
///
/// Compatible with PostgreSQL's pg_dump.
#[derive(Parser)]
#[command(
    name = "pg_dump",
    version = pg_dump_version(),
    about = "pg_dump dumps a database as a text file or to other formats."
)]
struct Cli {
    /// Database name to dump (positional, alternative to -d)
    #[arg()]
    dbname: Vec<String>,

    /// Database name or connection string
    #[arg(short = 'd', long = "dbname")]
    dbname_flag: Option<String>,

    /// Output format: plain (p), custom (c), directory (d), tar (t)
    #[arg(short = 'F', long = "format")]
    format: Option<String>,

    /// Output file or directory name
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// Dump only the schema (no data)
    #[arg(short = 's', long = "schema-only")]
    schema_only: bool,

    /// Dump only the data (no schema)
    #[arg(short = 'a', long = "data-only")]
    data_only: bool,

    /// Dump only statistics (no schema or data)
    #[arg(long = "statistics-only")]
    statistics_only: bool,

    /// Do not dump statistics
    #[arg(long = "no-statistics")]
    no_statistics: bool,

    /// Include foreign-server data
    #[arg(long = "include-foreign-data")]
    include_foreign_data: Option<String>,

    /// Drop database objects before recreating them
    #[arg(short = 'c', long = "clean")]
    clean: bool,

    /// Use DROP ... IF EXISTS
    #[arg(long = "if-exists")]
    if_exists: bool,

    /// Number of parallel jobs for directory format
    #[arg(short = 'j', long = "jobs", allow_negative_numbers = true)]
    jobs: Option<String>,

    /// Compression specification (algorithm[:level] or just level)
    #[arg(short = 'Z', long = "compress")]
    compress: Option<String>,

    /// Extra float digits
    #[arg(long = "extra-float-digits", allow_negative_numbers = true)]
    extra_float_digits: Option<i64>,

    /// Rows per INSERT statement
    #[arg(long = "rows-per-insert")]
    rows_per_insert: Option<i64>,

    /// Use INSERT commands instead of COPY
    #[arg(long = "inserts")]
    inserts: bool,

    /// Use INSERT commands with column names
    #[arg(long = "column-inserts")]
    column_inserts: bool,

    /// Add ON CONFLICT DO NOTHING to INSERT commands
    #[arg(long = "on-conflict-do-nothing")]
    on_conflict_do_nothing: bool,
}

/// Build the version string: `pg_dump (pg_plumbing) <version>`.
fn pg_dump_version() -> &'static str {
    concat!("pg_dump (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

fn validate_format(fmt: &str) -> bool {
    matches!(
        fmt,
        "plain" | "p" | "custom" | "c" | "directory" | "d" | "tar" | "t"
    )
}

fn main() {
    let cli = Cli::parse();

    // Too many positional args
    if cli.dbname.len() > 1 {
        eprintln!(
            "pg_dump: too many command-line arguments (first is \"{}\")",
            cli.dbname[1]
        );
        std::process::exit(1);
    }

    // Validate format if provided
    if let Some(ref fmt) = cli.format {
        if !validate_format(fmt) {
            eprintln!("pg_dump: invalid output format \"{fmt}\"");
            std::process::exit(1);
        }
    }

    let format_str = cli.format.as_deref().unwrap_or("plain");

    // --schema-only + --data-only
    if cli.schema_only && cli.data_only {
        eprintln!("pg_dump: options -s/--schema-only and -a/--data-only cannot be used together");
        std::process::exit(1);
    }

    // --schema-only + --statistics-only
    if cli.schema_only && cli.statistics_only {
        eprintln!(
            "pg_dump: options -s/--schema-only and --statistics-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --data-only + --statistics-only
    if cli.data_only && cli.statistics_only {
        eprintln!("pg_dump: options -a/--data-only and --statistics-only cannot be used together");
        std::process::exit(1);
    }

    // --statistics-only + --no-statistics
    if cli.statistics_only && cli.no_statistics {
        eprintln!("pg_dump: options --statistics-only and --no-statistics cannot be used together");
        std::process::exit(1);
    }

    // --include-foreign-data + --schema-only
    if cli.include_foreign_data.is_some() && cli.schema_only {
        eprintln!(
            "pg_dump: options --include-foreign-data and -s/--schema-only cannot be used together"
        );
        std::process::exit(1);
    }

    // --include-foreign-data + -j
    if cli.include_foreign_data.is_some() && cli.jobs.is_some() {
        eprintln!("pg_dump: options --include-foreign-data and --jobs cannot be used together");
        std::process::exit(1);
    }

    // --clean + --data-only
    if cli.clean && cli.data_only {
        eprintln!("pg_dump: options -c/--clean and -a/--data-only cannot be used together");
        std::process::exit(1);
    }

    // --if-exists requires --clean
    if cli.if_exists && !cli.clean {
        eprintln!("pg_dump: option --if-exists requires option -c/--clean");
        std::process::exit(1);
    }

    // --on-conflict-do-nothing requires --inserts, --rows-per-insert, or --column-inserts
    if cli.on_conflict_do_nothing
        && !cli.inserts
        && !cli.column_inserts
        && cli.rows_per_insert.is_none()
    {
        eprintln!(
            "pg_dump: option --on-conflict-do-nothing requires option --inserts, --column-inserts, or --rows-per-insert"
        );
        std::process::exit(1);
    }

    // Validate jobs
    if let Some(ref jobs_str) = cli.jobs {
        match jobs_str.parse::<i64>() {
            Ok(n) if !(1..=1000).contains(&n) => {
                eprintln!("pg_dump: invalid number of parallel jobs: {n}");
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!("pg_dump: invalid number of parallel jobs: \"{jobs_str}\"");
                std::process::exit(1);
            }
            Ok(_) => {}
        }

        // -j requires directory format
        let is_directory = matches!(format_str, "directory" | "d");
        if !is_directory {
            eprintln!("pg_dump: parallel backup only supported by the directory format");
            std::process::exit(1);
        }
    }

    // Validate --compress
    if let Some(ref compress_str) = cli.compress {
        validate_compress(compress_str, format_str);
    }

    // --extra-float-digits range: -15 to 3
    if let Some(v) = cli.extra_float_digits {
        if !(-15..=3).contains(&v) {
            eprintln!("pg_dump: --extra-float-digits must be in range -15..3, got {v}");
            std::process::exit(1);
        }
    }

    // --rows-per-insert must be >= 1
    if let Some(v) = cli.rows_per_insert {
        if v < 1 {
            eprintln!("pg_dump: --rows-per-insert must be a value >= 1");
            std::process::exit(1);
        }
    }

    // Resolve dbname.
    let dbname = if !cli.dbname.is_empty() {
        cli.dbname[0].clone()
    } else {
        cli.dbname_flag
            .clone()
            .or_else(|| std::env::var("PGDATABASE").ok())
            .unwrap_or_else(|| "postgres".to_string())
    };

    let opts = dump::DumpOptions {
        dbname,
        tables: Vec::new(), // TODO: -t flag not yet plumbed in pg_dump binary
        schema_only: cli.schema_only,
        data_only: cli.data_only,
        inserts: cli.inserts || cli.column_inserts || cli.rows_per_insert.is_some(),
        column_inserts: cli.column_inserts,
        rows_per_insert: cli.rows_per_insert.map(|v| v as u32),
        schemas: Vec::new(),
        exclude_schemas: Vec::new(),
        exclude_tables: Vec::new(),
        no_owner: false,
        no_privileges: false,
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    match format_str {
        "custom" | "c" => {
            let bytes = rt.block_on(dump::dump_custom(&opts)).unwrap_or_else(|e| {
                eprintln!("pg_dump: {e}");
                std::process::exit(1);
            });
            match cli.file {
                Some(ref path) => {
                    std::fs::write(path, &bytes).unwrap_or_else(|e| {
                        eprintln!("pg_dump: could not write to file \"{path}\": {e}");
                        std::process::exit(1);
                    });
                }
                None => {
                    use std::io::Write;
                    std::io::stdout().write_all(&bytes).unwrap_or_else(|e| {
                        eprintln!("pg_dump: write error: {e}");
                        std::process::exit(1);
                    });
                }
            }
        }
        _ => {
            // Plain format (and unimplemented formats fall back to plain).
            let output = rt.block_on(dump::dump_plain(&opts)).unwrap_or_else(|e| {
                eprintln!("pg_dump: {e}");
                std::process::exit(1);
            });
            match cli.file {
                Some(ref path) => {
                    std::fs::write(path, &output).unwrap_or_else(|e| {
                        eprintln!("pg_dump: could not write to file \"{path}\": {e}");
                        std::process::exit(1);
                    });
                }
                None => print!("{output}"),
            }
        }
    }
}

fn validate_compress(compress_str: &str, format_str: &str) {
    // Compression is not supported by tar format
    let is_tar = matches!(format_str, "tar" | "t");

    // Parse compress spec: algorithm[:level] or just integer level
    // Allowed algorithms: gzip, zstd, lz4, none, zlib
    let has_colon = compress_str.contains(':');
    let starts_with_digit = compress_str
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '-');

    let (algorithm, level_str): (Option<&str>, Option<&str>) = if has_colon {
        let idx = compress_str.find(':').unwrap();
        let alg = &compress_str[..idx];
        let lvl = &compress_str[idx + 1..];
        (Some(alg), Some(lvl))
    } else if starts_with_digit {
        // pure integer (old -Z N style)
        (None, Some(compress_str))
    } else {
        // pure algorithm name, no level
        (Some(compress_str), None)
    };

    if let Some(alg) = algorithm {
        let valid_algorithms = ["gzip", "zstd", "lz4", "none", "zlib", ""];
        if !valid_algorithms.contains(&alg) {
            eprintln!("pg_dump: unrecognized compression algorithm: \"{alg}\"");
            std::process::exit(1);
        }

        // "none" does not accept a compression level > 0
        if alg == "none" {
            if let Some(lvl) = level_str {
                match lvl.parse::<i64>() {
                    Ok(n) if n > 0 => {
                        eprintln!(
                            "pg_dump: compression algorithm \"none\" does not accept a compression level"
                        );
                        std::process::exit(1);
                    }
                    Err(_) if !lvl.is_empty() => {
                        eprintln!(
                            "pg_dump: invalid compression level \"{lvl}\" for algorithm \"none\""
                        );
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
        }

        // gzip / zlib level must be 0-9
        if alg == "gzip" || alg == "zlib" {
            if let Some(lvl) = level_str {
                match lvl.parse::<i64>() {
                    Ok(n) if !(0..=9).contains(&n) => {
                        eprintln!("pg_dump: compression level {n} is out of range (0..9) for gzip");
                        std::process::exit(1);
                    }
                    Err(_) => {
                        eprintln!(
                            "pg_dump: invalid compression level \"{lvl}\" for algorithm \"{alg}\""
                        );
                        std::process::exit(1);
                    }
                    Ok(_) => {}
                }
            }
        }

        // Tar format doesn't support compression
        if is_tar && alg != "none" && !alg.is_empty() {
            if let Some(lvl) = level_str {
                match lvl.parse::<i64>() {
                    Ok(n) if n > 0 => {
                        eprintln!("pg_dump: compression is not supported by tar archive format");
                        std::process::exit(1);
                    }
                    _ => {}
                }
            } else {
                // algorithm specified without level means use default compression
                eprintln!("pg_dump: compression is not supported by tar archive format");
                std::process::exit(1);
            }
        }
    } else {
        // Old-style: just a level integer (implies gzip)
        if let Some(lvl) = level_str {
            match lvl.parse::<i64>() {
                Ok(n) if !(0..=9).contains(&n) => {
                    eprintln!("pg_dump: compression level {n} is out of range (0..9) for gzip");
                    std::process::exit(1);
                }
                Err(_) => {
                    eprintln!("pg_dump: invalid compression level \"{lvl}\"");
                    std::process::exit(1);
                }
                Ok(_) => {}
            }

            // Tar format doesn't support compression
            if is_tar {
                match lvl.parse::<i64>() {
                    Ok(n) if n > 0 => {
                        eprintln!("pg_dump: compression is not supported by tar archive format");
                        std::process::exit(1);
                    }
                    _ => {}
                }
            }
        }
    }
}
