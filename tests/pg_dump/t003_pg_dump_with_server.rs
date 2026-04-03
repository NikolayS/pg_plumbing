// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/003_pg_dump_with_server.pl
//!
//! These tests verify that dumping foreign data (--include-foreign-data)
//! correctly includes only foreign tables belonging to matching servers,
//! and that a dummy FDW without a handler fails with the expected error.
//! Requires a running PostgreSQL instance.

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
