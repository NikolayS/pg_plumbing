// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_dump — dump a PostgreSQL database.

use clap::Parser;

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
    /// Database name to dump
    dbname: Option<String>,
}

/// Build the version string: `pg_dump (pg_plumbing) <version>`.
fn pg_dump_version() -> &'static str {
    concat!("pg_dump (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();

    // Dump logic not yet implemented.
    eprintln!("pg_dump: not yet implemented");
    std::process::exit(1);
}
