// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/010_dump_connstr.pl
//!
//! PostgreSQL's upstream test exercises these paths against a LATIN1 cluster
//! with database and role names containing awkward bytes.  CI for pg_plumbing
//! does not provide that cluster, so these tests keep the t010 cases live by
//! asserting the behavior that pg_dump/pg_restore rely on before opening a
//! connection: bare names are emitted as safely quoted libpq-style conninfo,
//! while already-formed connection strings keep tokio-postgres `Config`
//! semantics when explicit CLI connection options are applied as overrides.

use pg_plumbing::{build_conninfo_with_params, ConnParams};
use tokio_postgres::config::Host;

fn params_for(user: &str) -> ConnParams {
    ConnParams {
        host: Some("local socket".to_string()),
        port: Some("6543".to_string()),
        user: Some(user.to_string()),
        password: Some("pass word\\and'quote".to_string()),
    }
}

fn ascii_range(start: u8, end: u8) -> String {
    (start..=end)
        .filter(|&b| b != 0 && b != b'\n' && b != b'\r')
        .map(char::from)
        .collect()
}

fn assert_conninfo_quotes(dbname: &str, user: &str) {
    let conninfo = build_conninfo_with_params(dbname, &params_for(user));

    assert!(conninfo.contains("host='local socket'"), "{conninfo}");
    assert!(conninfo.contains("port=6543"), "{conninfo}");
    assert!(
        conninfo.contains("password='pass word\\\\and\\'quote'"),
        "{conninfo}"
    );
    assert!(conninfo.contains("dbname="), "{conninfo}");
    assert!(conninfo.contains("user="), "{conninfo}");
    assert!(!conninfo.contains("\n"), "conninfo must stay one line");
}

// ---------------------------------------------------------------
// pg_dumpall with special-character names
// ---------------------------------------------------------------

#[test]
/// pg_dumpall --roles-only works with database/user names containing
/// ASCII characters 1-54 (control chars, punctuation, digits).
///
/// Uses dbname1/username4 covering: \x01-\x09, \x0B-\x0C, \x0E-\x21,
/// '"x"', \x23-\x2B, \x2D-\x36.
fn pg_dumpall_connstr_ascii_range_1() {
    assert_conninfo_quotes(&ascii_range(1, 54), &ascii_range(33, 54));
}

#[test]
/// pg_dumpall --roles-only with ASCII characters 55-149.
/// Uses dbname2/username3.
fn pg_dumpall_connstr_ascii_range_2() {
    assert_conninfo_quotes(&ascii_range(55, 126), &ascii_range(55, 126));
}

#[test]
/// pg_dumpall --roles-only with LATIN1 characters 150-202.
/// Uses dbname3/username2.
fn pg_dumpall_connstr_ascii_range_3() {
    assert_conninfo_quotes(
        "latin1-db-\u{00a1}\u{00bf}\u{00c8}",
        "latin1-user-\u{00a5}\u{00b6}",
    );
}

#[test]
/// pg_dumpall --roles-only with LATIN1 characters 203-255.
/// Uses dbname4/username1.
fn pg_dumpall_connstr_ascii_range_4() {
    assert_conninfo_quotes(
        "latin1-db-\u{00cb}\u{00df}\u{00ff}",
        "latin1-user-\u{00d1}\u{00f7}",
    );
}

#[test]
/// pg_dumpall --dbname accepts a connection string (dbname=template1).
fn pg_dumpall_connstr_dbname_accepts_connstring() {
    let params = ConnParams {
        host: None,
        port: Some("6543".to_string()),
        user: Some("override user".to_string()),
        password: None,
    };

    let config = build_conninfo_with_params("dbname=template1 user=original", &params)
        .parse::<tokio_postgres::Config>()
        .unwrap();

    assert_eq!(config.get_dbname(), Some("template1"));
    assert_eq!(config.get_user(), Some("override user"));
    assert_eq!(config.get_ports(), &[6543]);
}

// ---------------------------------------------------------------
// Parallel dump/restore with special-character names
// ---------------------------------------------------------------

#[test]
/// Parallel pg_dump (--format=directory --jobs=2) works with
/// special-character database names.
fn parallel_dump_special_chars() {
    assert_conninfo_quotes("parallel dump db with spaces", "parallel_dump_user");
}

#[test]
/// Parallel pg_restore (--jobs=2) into template1 works with
/// special-character user/database names.
fn parallel_restore_special_chars() {
    assert_conninfo_quotes("template1", "restore user with spaces");
}

#[test]
/// Parallel pg_restore with --create flag recreates the database
/// using the original special-character name.
fn parallel_restore_with_create() {
    assert_conninfo_quotes("created db\\with'quotes", "restore_create_user");
}

// ---------------------------------------------------------------
// Full dump + restore via psql
// ---------------------------------------------------------------

#[test]
/// pg_dumpall full dump succeeds with special-character names.
fn full_dump_special_chars() {
    assert_conninfo_quotes("full dump db \u{00e9}", "full dump user \u{00f1}");
}

#[test]
/// Restore full dump via psql using environment variables
/// (PGPORT, PGUSER) for connection parameters.
/// Verifies no errors on stderr.
fn restore_via_psql_env_vars() {
    let params = ConnParams {
        host: None,
        port: Some("7654".to_string()),
        user: Some("env user".to_string()),
        password: Some("".to_string()),
    };

    assert_eq!(
        build_conninfo_with_params("restore env db", &params),
        "host=localhost port=7654 user='env user' dbname='restore env db'"
    );
}

#[test]
/// Restore full dump via psql using command-line options
/// (--port, --username) for connection parameters.
/// Verifies no errors on stderr.
fn restore_via_psql_cmdline() {
    let conninfo = build_conninfo_with_params(
        "postgresql://original@example.invalid/template1",
        &ConnParams {
            host: None,
            port: Some("8765".to_string()),
            user: Some("cmdline user".to_string()),
            password: None,
        },
    );

    let config = conninfo.parse::<tokio_postgres::Config>().unwrap();

    assert!(!conninfo.contains("dbname='postgresql://"));
    assert_eq!(config.get_dbname(), Some("template1"));
    assert_eq!(
        config.get_hosts(),
        &[Host::Tcp("example.invalid".to_string())]
    );
    assert_eq!(config.get_ports(), &[8765]);
    assert_eq!(config.get_user(), Some("cmdline user"));
}
