// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/003_pg_dump_with_server.pl
//!
//! These tests verify dump behavior that requires a running PostgreSQL
//! instance. Adapted for pg_plumbing's plain-format dump.

#[test]
/// Dump to a file using -f flag produces the same output as stdout.
/// Un-ignored: tests -f / --file flag with a running server.
fn dump_to_file_matches_stdout() {
    crate::common::setup_test_schema();

    // Dump to stdout.
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres"]);
    assert_eq!(code, 0);

    // Dump to file.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("dump.sql");
    let path_str = path.to_string_lossy().to_string();
    let (_stdout2, _stderr2, code2) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres", "-f", &path_str]);
    assert_eq!(code2, 0);

    let file_content = std::fs::read_to_string(&path).expect("read dump file");
    assert_eq!(
        stdout, file_content,
        "file output should match stdout output"
    );
}

#[test]
/// Column-inserts mode includes column names in INSERT statements.
/// Un-ignored: tests --column-inserts flag.
fn column_inserts_include_column_names() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--column-inserts",
    ]);
    assert_eq!(code, 0);
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple (id, name, value) VALUES"),
        "column-inserts should include column names:\n{stdout}"
    );
}

#[test]
#[ignore] // RED — not yet implemented
/// Dumping a foreign table from a dummy FDW (no handler) fails with an
/// error mentioning the FDW and the table.
///
/// Setup:
///   CREATE FOREIGN DATA WRAPPER dummy;
///   CREATE SERVER s0 FOREIGN DATA WRAPPER dummy;
///   CREATE SERVER s1 FOREIGN DATA WRAPPER dummy;
///   CREATE SERVER s2 FOREIGN DATA WRAPPER dummy;
///   CREATE FOREIGN TABLE t0 (a int) SERVER s0;
///   CREATE FOREIGN TABLE t1 (a int) SERVER s1;
///
/// Command: pg_dump --include-foreign-data=s0 postgres
/// Expected: error containing 'foreign-data wrapper "dummy" has no handler'
///           and mentioning table t0.
fn foreign_data_dump_fails_on_dummy_fdw() {}

#[test]
#[ignore]
/// Dumping a foreign server that has no tables succeeds.
///
/// Command: pg_dump --data-only --include-foreign-data=s2 postgres
/// Expected: exit 0 (s2 has no foreign tables).
fn foreign_data_dump_empty_server_succeeds() {}
