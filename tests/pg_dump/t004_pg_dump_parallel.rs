// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/004_pg_dump_parallel.pl
//!
//! These tests verify parallel dump and restore with --jobs, including
//! tables with hash-partitioned data that exercise the parallel worker
//! data-ordering logic.  Requires a running PostgreSQL instance.

#[test]
/// Parallel dump of a database with plain table, safe hash-partitioned
/// table, and dangerous hash-partitioned table (enum partition key).
///
/// Setup:
///   - create type digit as enum ('0'..'9')
///   - create table tplain (en digit, data int unique) + 100 rows
///   - create table ths (mod int, data int) partition by hash(mod)
///     with 3 partitions + 300 rows
///   - create table tht (en digit, data int) partition by hash(en)
///     with 3 partitions + 300 rows  (dangerous: enum hash)
///
/// Command: pg_dump --format=directory --jobs=2 --file=dump1
/// Expected: exit 0
fn parallel_dump() {
    crate::common::setup_parallel_test_schema();
    let dump_dir = format!("/tmp/pg_plumbing_parallel_dump1_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dump_dir);

    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "--format=directory",
        "--jobs=2",
        "--file",
        &dump_dir,
        "-d",
        "postgres",
    ]);
    assert_eq!(code, 0, "parallel dump failed: {stderr}");
    assert!(std::path::Path::new(&format!("{dump_dir}/toc.dat")).exists());

    let _ = std::fs::remove_dir_all(&dump_dir);
}

#[test]
/// Parallel restore from directory format into a fresh database.
///
/// Command: pg_restore --jobs=3 --dbname=dest1 dump1
/// Expected: exit 0, data matches source.
fn parallel_restore() {
    crate::common::setup_parallel_test_schema();
    let dump_dir = format!("/tmp/pg_plumbing_parallel_restore1_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dump_dir);

    // dump
    let (_, _, code) = crate::common::run_pg_dump(&[
        "--format=directory",
        "--jobs=2",
        "--file",
        &dump_dir,
        "-d",
        "postgres",
    ]);
    assert_eq!(code, 0, "parallel dump failed");

    // restore
    let dest_db = format!("pg_plumbing_parallel_dest1_{}", std::process::id());
    crate::common::create_test_db(&dest_db);

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--jobs=3", "--dbname", &dest_db, &dump_dir])
        .env("PGPASSWORD", "postgres")
        .status()
        .unwrap();
    assert!(status.success(), "parallel restore failed");

    // verify row counts
    let count = crate::common::psql_query(&dest_db, "SELECT COUNT(*) FROM tplain");
    assert_eq!(count.trim(), "100", "tplain row count mismatch: {count}");

    crate::common::drop_test_db(&dest_db);
    let _ = std::fs::remove_dir_all(&dump_dir);
}

#[test]
/// Parallel dump with --inserts mode (instead of COPY).
///
/// Command: pg_dump --format=directory --jobs=2 --inserts --file=dump2
/// Expected: exit 0
fn parallel_dump_inserts() {
    crate::common::setup_parallel_test_schema();
    let dump_dir = format!("/tmp/pg_plumbing_parallel_dump2_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dump_dir);

    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "--format=directory",
        "--jobs=2",
        "--inserts",
        "--file",
        &dump_dir,
        "-d",
        "postgres",
    ]);
    assert_eq!(code, 0, "parallel inserts dump failed: {stderr}");
    assert!(std::path::Path::new(&format!("{dump_dir}/toc.dat")).exists());

    let _ = std::fs::remove_dir_all(&dump_dir);
}

#[test]
/// Parallel restore of an inserts-mode dump.
///
/// Command: pg_restore --jobs=3 --dbname=dest2 dump2
/// Expected: exit 0, data matches source.
fn parallel_restore_inserts() {
    crate::common::setup_parallel_test_schema();
    let dump_dir = format!(
        "/tmp/pg_plumbing_parallel_dump_inserts_{}",
        std::process::id()
    );
    let _ = std::fs::remove_dir_all(&dump_dir);

    let (_, _, code) = crate::common::run_pg_dump(&[
        "--format=directory",
        "--jobs=2",
        "--inserts",
        "--file",
        &dump_dir,
        "-d",
        "postgres",
    ]);
    assert_eq!(code, 0);

    let dest_db = format!("pg_plumbing_parallel_dest2_{}", std::process::id());
    crate::common::create_test_db(&dest_db);

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--jobs=3", "--dbname", &dest_db, &dump_dir])
        .env("PGPASSWORD", "postgres")
        .status()
        .unwrap();
    assert!(status.success(), "parallel restore (inserts mode) failed");

    let count = crate::common::psql_query(&dest_db, "SELECT COUNT(*) FROM tplain");
    assert_eq!(count.trim(), "100");

    crate::common::drop_test_db(&dest_db);
    let _ = std::fs::remove_dir_all(&dump_dir);
}
