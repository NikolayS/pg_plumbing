// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests for issue #21 — duplicate sequence DDL entries in directory format.
//!
//! When two tables share a sequence (i.e., `pg_depend` has `deptype='a'`
//! entries linking one sequence to two different tables), the directory-format
//! dump must emit exactly ONE SEQUENCE entry in toc.dat — not one per
//! referencing table.
//!
//! Bug 1 (MEDIUM — this test): Duplicate SEQUENCE TOC entries caused
//! `CREATE SEQUENCE` to run twice on restore, producing:
//!   ERROR: relation "public.shared_id_seq" already exists
//!
//! Bug 2 (LOW — already fixed): Filename collision for sequences with the
//! same name in different schemas. Filenames use the `schema__name.seq.ddl`
//! format (double underscore as separator), ensuring uniqueness across
//! schemas. Confirmed correct in current code — no fix needed here.

use std::fs;
use std::path::Path;

/// Set up two tables that share a sequence by injecting a second `deptype='a'`
/// row into `pg_catalog.pg_depend`, simulating the scenario described in #21.
///
/// Background: in PostgreSQL a sequence can normally only be OWNED BY one
/// column (one `a`-type dependency). The duplicate arises when `pg_depend`
/// has two `a`-type rows for the same sequence pointing to different tables —
/// e.g., after manual ownership reassignment or import from another tool.
///
/// Uses OnceLock to be idempotent across repeated test runs.
fn setup_shared_sequence_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = crate::common::test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();

        // Step 1: create the base objects.
        let setup_sql = "
            DROP TABLE IF EXISTS shared_seq_tbl_b CASCADE;
            DROP TABLE IF EXISTS shared_seq_tbl_a CASCADE;
            DROP SEQUENCE IF EXISTS public.issue21_seq CASCADE;
            CREATE SEQUENCE public.issue21_seq START 1;
            CREATE TABLE shared_seq_tbl_a (
                id integer NOT NULL DEFAULT nextval('public.issue21_seq'),
                label text
            );
            ALTER SEQUENCE public.issue21_seq OWNED BY shared_seq_tbl_a.id;
            CREATE TABLE shared_seq_tbl_b (
                id integer NOT NULL DEFAULT nextval('public.issue21_seq'),
                label text
            );
            INSERT INTO shared_seq_tbl_a (label) VALUES ('alpha'), ('beta');
            INSERT INTO shared_seq_tbl_b (label) VALUES ('gamma');
        ";

        run_psql(&conninfo, &password, setup_sql, "setup base objects");

        // Step 2: inject a second `deptype='a'` row so that the sequence appears
        // when querying dependencies for BOTH tables.  This is the exact condition
        // that triggers the duplicate-emission bug in dump_directory.
        let inject_sql = "
            INSERT INTO pg_depend (classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype)
            SELECT
              (SELECT oid FROM pg_class WHERE relname = 'pg_class'),
              (SELECT oid FROM pg_class WHERE relname = 'issue21_seq'),
              0,
              (SELECT oid FROM pg_class WHERE relname = 'pg_class'),
              (SELECT oid FROM pg_class WHERE relname = 'shared_seq_tbl_b'),
              0,
              'a'
            WHERE NOT EXISTS (
              SELECT 1 FROM pg_depend
              WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'issue21_seq')
                AND refobjid = (SELECT oid FROM pg_class WHERE relname = 'shared_seq_tbl_b')
                AND deptype  = 'a'
            );
        ";

        run_psql(&conninfo, &password, inject_sql, "inject duplicate pg_depend row");
    });
}

fn run_psql(conninfo: &str, password: &str, sql: &str, label: &str) {
    let mut cmd = std::process::Command::new("psql");
    cmd.arg(conninfo).arg("-c").arg(sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", password);
    }
    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("psql for {label} failed to spawn: {e}"));
    assert!(
        output.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
/// Directory-format dump of tables sharing a sequence must produce exactly
/// ONE SEQUENCE entry in toc.dat (not one per referencing table).
///
/// Without the fix, toc.dat contains:
///   SEQUENCE public.issue21_seq public__issue21_seq.seq.ddl
///   SEQUENCE public.issue21_seq public__issue21_seq.seq.ddl   ← duplicate!
///
/// On restore that causes:
///   ERROR: relation "public.issue21_seq" already exists
///
/// Regression test for issue #21.
fn dir_format_shared_sequence_dedup_toc() {
    setup_shared_sequence_schema();

    let dump_dir = format!("/tmp/pg_plumbing_dedup_seq_{}", std::process::id());
    let _ = fs::remove_dir_all(&dump_dir);

    // Dump both tables (they share public.issue21_seq via pg_depend).
    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "-F",
        "directory",
        "-t",
        "shared_seq_tbl_a",
        "-t",
        "shared_seq_tbl_b",
        "-d",
        "postgres",
        "-f",
        &dump_dir,
    ]);
    assert_eq!(code, 0, "pg_dump -F directory failed: {stderr}");

    // Read toc.dat and count SEQUENCE entries for issue21_seq.
    let toc_path = format!("{dump_dir}/toc.dat");
    assert!(Path::new(&toc_path).exists(), "toc.dat should exist");

    let toc = fs::read_to_string(&toc_path).expect("failed to read toc.dat");
    let sequence_entries: Vec<&str> = toc
        .lines()
        .filter(|line| line.starts_with("SEQUENCE ") && line.contains("issue21_seq"))
        .collect();

    assert_eq!(
        sequence_entries.len(),
        1,
        "expected exactly 1 SEQUENCE entry for issue21_seq in toc.dat, found {}.\n\
         This is the regression for issue #21: duplicate TOC entries cause\n\
         CREATE SEQUENCE to run twice on restore.\n\
         toc.dat contents:\n{toc}",
        sequence_entries.len()
    );

    // Also verify the DDL file is present.
    // Note: Bug 2 (filename collision across schemas) is already fixed —
    // filenames use `schema__name.seq.ddl` format (double underscore separator).
    let seq_ddl_path = format!("{dump_dir}/public__issue21_seq.seq.ddl");
    assert!(
        Path::new(&seq_ddl_path).exists(),
        "sequence DDL file public__issue21_seq.seq.ddl should exist in dump dir"
    );

    let _ = fs::remove_dir_all(&dump_dir);
}

#[test]
/// Full round-trip: directory dump of shared-sequence tables restores without error.
///
/// Before the fix, restore would fail with:
///   ERROR: relation "public.issue21_seq" already exists
///
/// Regression test for issue #21.
fn dir_format_shared_sequence_restore_roundtrip() {
    setup_shared_sequence_schema();

    let dump_dir = format!("/tmp/pg_plumbing_dedup_seq_rt_{}", std::process::id());
    let _ = fs::remove_dir_all(&dump_dir);

    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "-F",
        "directory",
        "-t",
        "shared_seq_tbl_a",
        "-t",
        "shared_seq_tbl_b",
        "-d",
        "postgres",
        "-f",
        &dump_dir,
    ]);
    assert_eq!(code, 0, "pg_dump -F directory failed: {stderr}");

    let dest_db = format!("pg_plumbing_dedup_seq_dest_{}", std::process::id());
    crate::common::create_test_db(&dest_db);

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_pg_plumbing"))
        .args(["pg-restore", "-d", &dest_db, &dump_dir])
        .env("PGPASSWORD", "postgres")
        .status()
        .expect("pg_restore should run");

    // Count rows in both tables.
    let count_a = crate::common::psql_query(&dest_db, "SELECT COUNT(*) FROM shared_seq_tbl_a");
    let count_b = crate::common::psql_query(&dest_db, "SELECT COUNT(*) FROM shared_seq_tbl_b");

    crate::common::drop_test_db(&dest_db);
    let _ = fs::remove_dir_all(&dump_dir);

    assert!(
        status.success(),
        "pg_restore failed — likely duplicate CREATE SEQUENCE (issue #21)"
    );
    assert_eq!(count_a.trim(), "2", "shared_seq_tbl_a should have 2 rows");
    assert_eq!(count_b.trim(), "1", "shared_seq_tbl_b should have 1 row");
}
