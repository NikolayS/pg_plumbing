// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Shared library for pg_plumbing (pg_dump/pg_restore).

pub mod dump;
pub mod restore;

/// Build a libpq-style connection string from a database name.
///
/// If the input already looks like a connection string (contains `=`),
/// use it as-is. Otherwise, build a minimal `host=... dbname=...` string
/// using environment variables for host/port/user.
pub fn build_conninfo(dbname: &str) -> String {
    if dbname.contains('=') {
        return dbname.to_string();
    }

    let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
    let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("PGPASSWORD").unwrap_or_default();

    let mut s = format!("host={host} port={port} user={user} dbname={dbname}");
    if !password.is_empty() {
        s.push_str(&format!(" password={password}"));
    }
    s
}
