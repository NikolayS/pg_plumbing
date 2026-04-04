// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Basic pg_restore integration tests: dump -> restore -> verify round-trip.

#[test]
/// Dump a table, restore to a different database, verify data matches.
fn restore_plain_dump_round_trip() {
    crate::common::setup_restore_test_schema();

    // Dump the test table to a file.
    let dir = tempfile::tempdir().expect("tempdir");
    let dump_path = dir.path().join("dump.sql");
    let dump_path_str = dump_path.to_string_lossy().to_string();

    let (_stdout, stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_restore",
        "-d",
        "postgres",
        "-f",
        &dump_path_str,
    ]);
    assert_eq!(code, 0, "pg_dump failed: {stderr}");

    // Create a fresh target database.
    let target_db = "pg_plumbing_restore_test";
    crate::common::create_test_db(target_db);

    // Restore the dump into the target database.
    let (_stdout, stderr, code) = crate::common::run_pg_restore(&["-d", target_db, &dump_path_str]);
    assert_eq!(code, 0, "pg_restore failed: {stderr}");

    // Verify data matches.
    let source_data = crate::common::psql_query(
        "postgres",
        "select id, name, value from public.dump_test_restore order by id;",
    );
    let target_data = crate::common::psql_query(
        target_db,
        "select id, name, value from public.dump_test_restore order by id;",
    );
    assert_eq!(
        source_data, target_data,
        "restored data should match source"
    );

    // Verify row count.
    let count =
        crate::common::psql_query(target_db, "select count(*) from public.dump_test_restore;");
    assert_eq!(count.trim(), "3", "should have 3 rows");

    // Clean up.
    crate::common::drop_test_db(target_db);
}

#[test]
/// Restore with --clean drops and recreates objects.
fn restore_with_clean_flag() {
    crate::common::setup_restore_test_schema();

    // Dump the test table.
    let dir = tempfile::tempdir().expect("tempdir");
    let dump_path = dir.path().join("dump.sql");
    let dump_path_str = dump_path.to_string_lossy().to_string();

    let (_stdout, stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_restore",
        "-d",
        "postgres",
        "-f",
        &dump_path_str,
    ]);
    assert_eq!(code, 0, "pg_dump failed: {stderr}");

    // Create target database and populate with conflicting table.
    let target_db = "pg_plumbing_restore_clean_test";
    crate::common::create_test_db(target_db);
    crate::common::psql(
        target_db,
        "create table public.dump_test_restore (id integer);",
    );

    // Restore with --clean should drop existing table and recreate.
    let (_stdout, stderr, code) =
        crate::common::run_pg_restore(&["-d", target_db, "--clean", &dump_path_str]);
    assert_eq!(code, 0, "pg_restore --clean failed: {stderr}");

    // Verify data is correct.
    let count =
        crate::common::psql_query(target_db, "select count(*) from public.dump_test_restore;");
    assert_eq!(count.trim(), "3", "should have 3 rows after clean restore");

    // Clean up.
    crate::common::drop_test_db(target_db);
}

#[test]
/// Restore from stdin using "-" as filename.
fn restore_from_stdin() {
    crate::common::setup_restore_test_schema();

    // Dump the test table to a file first.
    let dir = tempfile::tempdir().expect("tempdir");
    let dump_path = dir.path().join("dump.sql");
    let dump_path_str = dump_path.to_string_lossy().to_string();

    let (_stdout, stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_restore",
        "-d",
        "postgres",
        "-f",
        &dump_path_str,
    ]);
    assert_eq!(code, 0, "pg_dump failed: {stderr}");

    // Create target database.
    let target_db = "pg_plumbing_restore_stdin_test";
    crate::common::create_test_db(target_db);

    // Restore by piping dump file through stdin.
    let bin = crate::common::pg_plumbing_bin();
    let dump_sql = std::fs::read(&dump_path).expect("read dump file");

    let mut cmd = std::process::Command::new(&bin);
    cmd.arg("pg-restore")
        .arg("-d")
        .arg(target_db)
        .arg("-")
        .stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn().expect("spawn pg_restore");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        stdin.write_all(&dump_sql).expect("write stdin");
    }
    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "pg_restore from stdin failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify data.
    let count =
        crate::common::psql_query(target_db, "select count(*) from public.dump_test_restore;");
    assert_eq!(count.trim(), "3", "should have 3 rows");

    // Clean up.
    crate::common::drop_test_db(target_db);
}

#[test]
/// pg_restore without -d flag fails with an error message.
fn restore_requires_dbname() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("dummy.sql");
    std::fs::write(&path, "SELECT 1;").expect("write");

    let (_stdout, stderr, code) = crate::common::run_pg_restore(&[&path.to_string_lossy()]);
    assert_ne!(code, 0, "should fail without -d");
    assert!(
        stderr.contains("no database specified"),
        "should mention missing database: {stderr}"
    );
}

#[test]
/// pg_restore without a filename fails with an error message.
fn restore_requires_filename() {
    let (_stdout, stderr, code) = crate::common::run_pg_restore(&["-d", "postgres"]);
    assert_ne!(code, 0, "should fail without filename");
    assert!(
        stderr.contains("no input file specified"),
        "should mention missing filename: {stderr}"
    );
}

#[test]
/// pg_restore --clean --if-exists: succeeds even when objects don't exist.
/// pg_restore --if-exists without --clean: fails with validation error.
fn restore_if_exists_integration() {
    crate::common::setup_restore_test_schema();

    // Dump the test table.
    let dir = tempfile::tempdir().expect("tempdir");
    let dump_path = dir.path().join("dump_if_exists.sql");
    let dump_path_str = dump_path.to_string_lossy().to_string();

    let (_stdout, stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_restore",
        "-d",
        "postgres",
        "-f",
        &dump_path_str,
    ]);
    assert_eq!(code, 0, "pg_dump failed: {stderr}");

    // Create target database.
    let target_db = "pg_plumbing_if_exists_test";
    crate::common::create_test_db(target_db);

    // ── Case 1: --clean --if-exists on an empty database ─────────────────
    // The DROP statements will fire but the objects won't exist.
    // With --if-exists, this must succeed (no error).
    let (_stdout, stderr, code) =
        crate::common::run_pg_restore(&["-d", target_db, "--clean", "--if-exists", &dump_path_str]);
    assert_eq!(
        code, 0,
        "pg_restore --clean --if-exists should succeed even when objects don't exist: {stderr}"
    );

    // Verify data was restored correctly.
    let count =
        crate::common::psql_query(target_db, "select count(*) from public.dump_test_restore;");
    assert_eq!(
        count.trim(),
        "3",
        "should have 3 rows after --clean --if-exists restore"
    );

    // ── Case 2: --if-exists without --clean should fail with a clear error ─
    let (_stdout, stderr, code) =
        crate::common::run_pg_restore(&["-d", target_db, "--if-exists", &dump_path_str]);
    assert_ne!(
        code, 0,
        "--if-exists without --clean must fail with an error"
    );
    assert!(
        stderr.contains("--if-exists requires") || stderr.contains("--clean"),
        "error should mention --if-exists requires --clean: {stderr}"
    );

    // Clean up.
    crate::common::drop_test_db(target_db);
}

#[test]
/// Restore a dump with INSERT statements (not COPY).
fn restore_inserts_mode_round_trip() {
    crate::common::setup_restore_test_schema();

    // Dump with --inserts.
    let dir = tempfile::tempdir().expect("tempdir");
    let dump_path = dir.path().join("dump_inserts.sql");
    let dump_path_str = dump_path.to_string_lossy().to_string();

    let (_stdout, stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_restore",
        "-d",
        "postgres",
        "--inserts",
        "-f",
        &dump_path_str,
    ]);
    assert_eq!(code, 0, "pg_dump --inserts failed: {stderr}");

    // Verify dump uses INSERT not COPY.
    let dump_content = std::fs::read_to_string(&dump_path).expect("read dump");
    assert!(
        dump_content.contains("INSERT INTO"),
        "dump should use INSERT statements"
    );

    // Create target and restore.
    let target_db = "pg_plumbing_restore_inserts_test";
    crate::common::create_test_db(target_db);

    let (_stdout, stderr, code) = crate::common::run_pg_restore(&["-d", target_db, &dump_path_str]);
    assert_eq!(code, 0, "pg_restore (inserts) failed: {stderr}");

    // Verify data.
    let source_data = crate::common::psql_query(
        "postgres",
        "select id, name, value from public.dump_test_restore order by id;",
    );
    let target_data = crate::common::psql_query(
        target_db,
        "select id, name, value from public.dump_test_restore order by id;",
    );
    assert_eq!(
        source_data, target_data,
        "restored INSERT data should match source"
    );

    // Clean up.
    crate::common::drop_test_db(target_db);
}
