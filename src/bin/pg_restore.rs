// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_restore — restore a PostgreSQL database from an archive.

use clap::Parser;

/// pg_restore restores a PostgreSQL database from an archive
/// created by pg_dump.
///
/// Compatible with PostgreSQL's pg_restore.
#[derive(Parser)]
#[command(
    name = "pg_restore",
    version = pg_restore_version(),
    about = "pg_restore restores a PostgreSQL database from an archive created by pg_dump."
)]
struct Cli {
    /// Archive file to restore
    filename: Option<String>,
}

/// Build the version string: `pg_restore (pg_plumbing) <version>`.
fn pg_restore_version() -> &'static str {
    concat!("pg_restore (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();

    // Restore logic not yet implemented.
    eprintln!("pg_restore: not yet implemented");
    std::process::exit(1);
}
