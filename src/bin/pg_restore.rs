// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_restore — restore a PostgreSQL database from an archive.

use anyhow::{bail, Result};
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
    /// Target database name or connection string.
    #[arg(short = 'd', long = "dbname")]
    dbname: Option<String>,

    /// Drop database objects before recreating them.
    #[arg(short = 'c', long = "clean")]
    clean: bool,

    /// Archive file to restore (positional).
    filename: Option<String>,
}

/// Build the version string: `pg_restore (pg_plumbing) <version>`.
fn pg_restore_version() -> &'static str {
    concat!("pg_restore (pg_plumbing) ", env!("CARGO_PKG_VERSION"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let dbname = match cli.dbname {
        Some(ref d) => d.clone(),
        None => bail!("pg_restore: no database specified (use -d)"),
    };

    let filename = match cli.filename {
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

    let opts = pg_plumbing::restore::RestoreOptions {
        dbname,
        clean: cli.clean,
    };

    pg_plumbing::restore::restore_plain(&sql, &opts).await
}
