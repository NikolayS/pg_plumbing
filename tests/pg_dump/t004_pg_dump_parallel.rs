// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/004_pg_dump_parallel.pl
//!
//! These tests verify parallel dump and restore with --jobs, including
//! tables with hash-partitioned data that exercise the parallel worker
//! data-ordering logic.  Requires a running PostgreSQL instance.

#[test]
#[ignore] // RED — not yet implemented
/// Parallel dump of a database with plain table, safe hash-partitioned
/// table, and dangerous hash-partitioned table (enum partition key).
///
/// Setup:
///   - create type digit as enum ('0'..'9')
///   - create table tplain (en digit, data int unique) + 1000 rows
///   - create table ths (mod int, data int) partition by hash(mod)
///     with 3 partitions + 1000 rows
///   - create table tht (en digit, data int) partition by hash(en)
///     with 3 partitions + 1000 rows  (dangerous: enum hash)
///
/// Command: pg_dump --format=directory --no-sync --jobs=2 --file=dump1
/// Expected: exit 0
fn parallel_dump() {}

#[test]
#[ignore]
/// Parallel restore from directory format into a fresh database.
///
/// Command: pg_restore --verbose --dbname=dest1 --jobs=3 dump1
/// Expected: exit 0, data matches source.
fn parallel_restore() {}

#[test]
#[ignore]
/// Parallel dump with --inserts mode (instead of COPY).
///
/// Command: pg_dump --format=directory --no-sync --jobs=2 --inserts
///          --file=dump2
/// Expected: exit 0
fn parallel_dump_inserts() {}

#[test]
#[ignore]
/// Parallel restore of an inserts-mode dump.
///
/// Command: pg_restore --verbose --dbname=dest2 --jobs=3 dump2
/// Expected: exit 0, data matches source.
fn parallel_restore_inserts() {}
