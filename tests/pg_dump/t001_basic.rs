// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/001_basic.pl
//!
//! These tests verify basic CLI behavior: --help, --version, option
//! validation, and detection of mutually exclusive flags.  They do NOT
//! require a running PostgreSQL instance.

// ---------------------------------------------------------------
// Basic program checks
// ---------------------------------------------------------------

#[test]
/// pg_dump --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_dump')
fn pg_dump_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("--help")
        .output()
        .expect("failed to run pg_dump");
    assert!(output.status.success(), "pg_dump --help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_dump"),
        "help output should mention pg_dump"
    );
    assert!(stdout.contains("Usage"), "help output should contain Usage");
}

#[test]
/// pg_dump --version exits 0 and prints version string.
/// Source: program_version_ok('pg_dump')
fn pg_dump_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("--version")
        .output()
        .expect("failed to run pg_dump");
    assert!(output.status.success(), "pg_dump --version should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_dump (pg_plumbing)"),
        "version output should contain 'pg_dump (pg_plumbing)'"
    );
}

#[test]
/// pg_dump rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_dump')
fn pg_dump_options_handling() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("--this-option-does-not-exist")
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump with unknown option should exit nonzero"
    );
}

#[test]
/// pg_restore --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_restore')
fn pg_restore_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .arg("--help")
        .output()
        .expect("failed to run pg_restore");
    assert!(output.status.success(), "pg_restore --help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_restore"),
        "help output should mention pg_restore"
    );
    assert!(stdout.contains("Usage"), "help output should contain Usage");
}

#[test]
/// pg_restore --version exits 0 and prints version string.
/// Source: program_version_ok('pg_restore')
fn pg_restore_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .arg("--version")
        .output()
        .expect("failed to run pg_restore");
    assert!(
        output.status.success(),
        "pg_restore --version should exit 0"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_restore (pg_plumbing)"),
        "version output should contain 'pg_restore (pg_plumbing)'"
    );
}

#[test]
/// pg_restore rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_restore')
fn pg_restore_options_handling() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .arg("--this-option-does-not-exist")
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore with unknown option should exit nonzero"
    );
}

#[test]
/// pg_dumpall --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_dumpall')
fn pg_dumpall_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .arg("--help")
        .output()
        .expect("failed to run pg_dumpall");
    assert!(output.status.success(), "pg_dumpall --help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_dumpall"),
        "help output should mention pg_dumpall"
    );
    assert!(stdout.contains("Usage"), "help output should contain Usage");
}

#[test]
/// pg_dumpall --version exits 0 and prints version string.
/// Source: program_version_ok('pg_dumpall')
fn pg_dumpall_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .arg("--version")
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        output.status.success(),
        "pg_dumpall --version should exit 0"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("pg_dumpall (pg_plumbing)"),
        "version output should contain 'pg_dumpall (pg_plumbing)'"
    );
}

#[test]
/// pg_dumpall rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_dumpall')
fn pg_dumpall_options_handling() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .arg("--this-option-does-not-exist")
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall with unknown option should exit nonzero"
    );
}

// ---------------------------------------------------------------
// Invalid option combinations — pg_dump
// ---------------------------------------------------------------

#[test]
/// pg_dump errors on too many command-line arguments.
/// `pg_dump qqq abc` → error: too many command-line arguments (first is "abc")
fn pg_dump_too_many_args() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["qqq", "abc"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump with too many args should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("too many command-line arguments"),
        "stderr should mention too many args, got: {stderr}"
    );
}

#[test]
/// -s/--schema-only and -a/--data-only cannot be used together.
/// `pg_dump -s -a` → error
fn pg_dump_schema_only_vs_data_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-s", "-a"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -s -a should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema-only") || stderr.contains("data-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -s/--schema-only and --statistics-only cannot be used together.
/// `pg_dump -s --statistics-only` → error
fn pg_dump_schema_only_vs_statistics_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-s", "--statistics-only"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -s --statistics-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema-only") || stderr.contains("statistics-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -a/--data-only and --statistics-only cannot be used together.
/// `pg_dump -a --statistics-only` → error
fn pg_dump_data_only_vs_statistics_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-a", "--statistics-only"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -a --statistics-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data-only") || stderr.contains("statistics-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --include-foreign-data and -s/--schema-only cannot be used together.
/// `pg_dump -s --include-foreign-data=xxx` → error
fn pg_dump_foreign_data_vs_schema_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-s", "--include-foreign-data=xxx"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -s --include-foreign-data=xxx should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("foreign-data") || stderr.contains("schema-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --statistics-only and --no-statistics cannot be used together.
/// `pg_dump --statistics-only --no-statistics` → error
fn pg_dump_statistics_only_vs_no_statistics() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--statistics-only", "--no-statistics"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --statistics-only --no-statistics should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("statistics"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --include-foreign-data is not supported with parallel backup.
/// `pg_dump -j2 --include-foreign-data=xxx` → error
fn pg_dump_foreign_data_vs_parallel() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-j2", "--include-foreign-data=xxx"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -j2 --include-foreign-data=xxx should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("foreign-data") || stderr.contains("jobs"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_dump -c -a` → error
fn pg_dump_clean_vs_data_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-c", "-a"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -c -a should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("clean") || stderr.contains("data-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --if-exists requires -c/--clean.
/// `pg_dump --if-exists` → error
fn pg_dump_if_exists_requires_clean() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("--if-exists")
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --if-exists should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("if-exists") || stderr.contains("clean"),
        "stderr should mention --if-exists requires --clean, got: {stderr}"
    );
}

#[test]
/// Parallel backup only supported by directory format.
/// `pg_dump -j3` → error (default format is plain, not directory)
fn pg_dump_parallel_requires_directory() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("-j3")
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -j3 (plain format) should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("directory") || stderr.contains("parallel"),
        "stderr should mention directory format required, got: {stderr}"
    );
}

#[test]
/// -j/--jobs must be in range (rejects negative values).
/// `pg_dump -j -1` → error
fn pg_dump_jobs_must_be_in_range() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-j", "-1"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -j -1 should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("jobs") || stderr.contains("parallel") || stderr.contains("range"),
        "stderr should mention invalid jobs, got: {stderr}"
    );
}

#[test]
/// Invalid output format is rejected.
/// `pg_dump -F garbage` → error: invalid output format
fn pg_dump_invalid_format() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-F", "garbage"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -F garbage should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("format") || stderr.contains("garbage"),
        "stderr should mention invalid format, got: {stderr}"
    );
}

#[test]
/// Unrecognized compression algorithm is rejected.
/// `pg_dump --compress garbage` → error
fn pg_dump_invalid_compress_algorithm() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--compress", "garbage"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --compress garbage should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("compress") || stderr.contains("algorithm") || stderr.contains("garbage"),
        "stderr should mention invalid compression algorithm, got: {stderr}"
    );
}

#[test]
/// Compression algorithm "none" does not accept a compression level.
/// `pg_dump --compress none:1` → error
fn pg_dump_compress_none_with_level() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--compress", "none:1"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --compress none:1 should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("none") || stderr.contains("compress"),
        "stderr should mention none compression level, got: {stderr}"
    );
}

#[test]
/// gzip compression level must be in valid range (1-9).
/// `pg_dump -Z 15` → error (requires libz)
fn pg_dump_gzip_level_out_of_range() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-Z", "15"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -Z 15 should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("range") || stderr.contains("compress") || stderr.contains("15"),
        "stderr should mention out of range, got: {stderr}"
    );
}

#[test]
/// Compression is not supported by tar archive format.
/// `pg_dump --compress 1 --format tar` → error (requires libz)
fn pg_dump_compress_vs_tar() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--compress", "1", "--format", "tar"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --compress 1 --format tar should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("tar") || stderr.contains("compress"),
        "stderr should mention tar compression not supported, got: {stderr}"
    );
}

#[test]
/// Non-integer compression option is rejected.
/// `pg_dump -Z gzip:nonInt` → error (requires libz)
fn pg_dump_compress_non_integer() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["-Z", "gzip:nonInt"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump -Z gzip:nonInt should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("compress") || stderr.contains("invalid") || stderr.contains("nonInt"),
        "stderr should mention invalid compression level, got: {stderr}"
    );
}

#[test]
/// --extra-float-digits must be in range.
/// `pg_dump --extra-float-digits -16` → error
fn pg_dump_extra_float_digits_range() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--extra-float-digits", "-16"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --extra-float-digits -16 should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("float") || stderr.contains("range"),
        "stderr should mention float digits range, got: {stderr}"
    );
}

#[test]
/// --rows-per-insert must be in range (rejects 0).
/// `pg_dump --rows-per-insert 0` → error
fn pg_dump_rows_per_insert_range() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .args(["--rows-per-insert", "0"])
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --rows-per-insert 0 should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("rows-per-insert") || stderr.contains("range"),
        "stderr should mention rows-per-insert range, got: {stderr}"
    );
}

#[test]
/// --on-conflict-do-nothing requires --inserts, --rows-per-insert,
/// or --column-inserts.
/// `pg_dump --on-conflict-do-nothing` → error
fn pg_dump_on_conflict_requires_inserts() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dump"))
        .arg("--on-conflict-do-nothing")
        .output()
        .expect("failed to run pg_dump");
    assert!(
        !output.status.success(),
        "pg_dump --on-conflict-do-nothing should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("on-conflict") || stderr.contains("inserts"),
        "stderr should mention on-conflict requires inserts, got: {stderr}"
    );
}

// ---------------------------------------------------------------
// Invalid option combinations — pg_restore
// ---------------------------------------------------------------

#[test]
/// pg_restore errors on too many command-line arguments.
/// `pg_restore qqq abc` → error: too many command-line arguments
fn pg_restore_too_many_args() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["qqq", "abc"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore with too many args should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("too many command-line arguments"),
        "stderr should mention too many args, got: {stderr}"
    );
}

#[test]
/// pg_restore requires one of -d/--dbname and -f/--file.
/// `pg_restore` (no args) → error
fn pg_restore_requires_dbname_or_file() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore with no args should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("dbname")
            || stderr.contains("file")
            || stderr.contains("must be specified"),
        "stderr should mention missing dbname/file, got: {stderr}"
    );
}

#[test]
/// -a/--data-only and -s/--schema-only cannot be used together.
/// `pg_restore -s -a -f -` → error
fn pg_restore_data_only_vs_schema_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-s", "-a", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -s -a -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data-only") || stderr.contains("schema-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -d/--dbname and -f/--file cannot be used together.
/// `pg_restore -d xxx -f xxx` → error
fn pg_restore_dbname_vs_file() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-d", "xxx", "-f", "xxx"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -d xxx -f xxx should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("dbname") || stderr.contains("file"),
        "stderr should mention conflicting -d/-f options, got: {stderr}"
    );
}

#[test]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_restore -c -a -f -` → error
fn pg_restore_clean_vs_data_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-c", "-a", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -c -a -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("clean") || stderr.contains("data-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --if-exists requires -c/--clean.
/// `pg_restore --if-exists -f -` → error
fn pg_restore_if_exists_requires_clean() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--if-exists", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --if-exists -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("if-exists") || stderr.contains("clean"),
        "stderr should mention --if-exists requires --clean, got: {stderr}"
    );
}

#[test]
/// -j/--jobs must be in range (rejects negative values).
/// `pg_restore -j -1 -f -` → error
fn pg_restore_jobs_must_be_in_range() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-j", "-1", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -j -1 -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("jobs") || stderr.contains("range") || stderr.contains("parallel"),
        "stderr should mention invalid jobs, got: {stderr}"
    );
}

#[test]
/// Cannot specify both --single-transaction and multiple jobs.
/// `pg_restore --single-transaction -j3 -f -` → error
fn pg_restore_single_transaction_vs_parallel() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--single-transaction", "-j3", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --single-transaction -j3 -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("single-transaction") || stderr.contains("jobs"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// Unrecognized archive format is rejected.
/// `pg_restore -f - -F garbage` → error
fn pg_restore_invalid_format() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-f", "-", "-F", "garbage"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -F garbage should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("format") || stderr.contains("garbage"),
        "stderr should mention invalid format, got: {stderr}"
    );
}

#[test]
/// Empty archive format string is rejected.
/// `pg_restore -f - -F ""` → error
fn pg_restore_empty_format() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-f", "-", "-F", ""])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -F \"\" should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("format"),
        "stderr should mention invalid format, got: {stderr}"
    );
}

#[test]
/// -C/--create and -1/--single-transaction cannot be used together.
/// `pg_restore -C -1 -f -` → error
fn pg_restore_create_vs_single_transaction() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-C", "-1", "-f", "-"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore -C -1 -f - should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("create") || stderr.contains("single-transaction"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --exclude-database and -g/--globals-only cannot be used together.
/// `pg_restore --exclude-database=foo --globals-only -d xxx` → error
fn pg_restore_exclude_database_vs_globals_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--exclude-database=foo", "--globals-only", "-d", "xxx"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --exclude-database=foo --globals-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("exclude-database") || stderr.contains("globals-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -a/--data-only and -g/--globals-only cannot be used together.
/// `pg_restore --data-only --globals-only -d xxx` → error
fn pg_restore_data_only_vs_globals_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--data-only", "--globals-only", "-d", "xxx"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --data-only --globals-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data-only") || stderr.contains("globals-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -g/--globals-only and -s/--schema-only cannot be used together.
/// `pg_restore --schema-only --globals-only -d xxx` → error
fn pg_restore_globals_only_vs_schema_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--schema-only", "--globals-only", "-d", "xxx"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --schema-only --globals-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("schema-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -g/--globals-only and --statistics-only cannot be used together.
/// `pg_restore --statistics-only --globals-only -d xxx` → error
fn pg_restore_globals_only_vs_statistics_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--statistics-only", "--globals-only", "-d", "xxx"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --statistics-only --globals-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("statistics-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --exclude-database can only be used with pg_dumpall archives.
/// `pg_restore --exclude-database=foo -d xxx dumpdir` → error
fn pg_restore_exclude_database_requires_dumpall_archive() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--exclude-database=foo", "-d", "xxx", "dumpdir"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --exclude-database=foo -d xxx dumpdir should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("exclude-database") || stderr.contains("dumpall"),
        "stderr should mention exclude-database requires dumpall archive, got: {stderr}"
    );
}

#[test]
/// --globals-only can only be used with pg_dumpall archives.
/// `pg_restore --globals-only -d xxx dumpdir` → error
fn pg_restore_globals_only_requires_dumpall_archive() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--globals-only", "-d", "xxx", "dumpdir"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --globals-only -d xxx dumpdir should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("dumpall"),
        "stderr should mention globals-only requires dumpall archive, got: {stderr}"
    );
}

#[test]
/// --globals-only and --no-globals cannot be used together.
/// `pg_restore --globals-only --no-globals -d xxx dumpdir` → error
fn pg_restore_globals_only_vs_no_globals() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["--globals-only", "--no-globals", "-d", "xxx", "dumpdir"])
        .output()
        .expect("failed to run pg_restore");
    assert!(
        !output.status.success(),
        "pg_restore --globals-only --no-globals should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("no-globals"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

// ---------------------------------------------------------------
// Invalid option combinations — pg_dumpall
// ---------------------------------------------------------------

#[test]
/// pg_dumpall errors on too many command-line arguments.
/// `pg_dumpall qqq abc` → error
fn pg_dumpall_too_many_args() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["qqq", "abc"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall with too many args should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("too many command-line arguments"),
        "stderr should mention too many args, got: {stderr}"
    );
}

#[test]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_dumpall -c -a` → error
fn pg_dumpall_clean_vs_data_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-c", "-a"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -c -a should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("clean") || stderr.contains("data-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -g/--globals-only and -r/--roles-only cannot be used together.
/// `pg_dumpall -g -r` → error
fn pg_dumpall_globals_only_vs_roles_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-g", "-r"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -g -r should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("roles-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -g/--globals-only and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall -g -t` → error
fn pg_dumpall_globals_only_vs_tablespaces_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-g", "-t"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -g -t should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("globals-only") || stderr.contains("tablespaces-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -r/--roles-only and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall -r -t` → error
fn pg_dumpall_roles_only_vs_tablespaces_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-r", "-t"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -r -t should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("roles-only") || stderr.contains("tablespaces-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --if-exists requires -c/--clean.
/// `pg_dumpall --if-exists` → error
fn pg_dumpall_if_exists_requires_clean() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .arg("--if-exists")
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --if-exists should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("if-exists") || stderr.contains("clean"),
        "stderr should mention --if-exists requires --clean, got: {stderr}"
    );
}

#[test]
/// --exclude-database and -g/--globals-only cannot be used together.
/// `pg_dumpall --exclude-database=foo --globals-only` → error
fn pg_dumpall_exclude_database_vs_globals_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--exclude-database=foo", "--globals-only"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --exclude-database=foo --globals-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("exclude-database") || stderr.contains("globals-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -a/--data-only and --no-data cannot be used together.
/// `pg_dumpall -a --no-data` → error
fn pg_dumpall_data_only_vs_no_data() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-a", "--no-data"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -a --no-data should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data-only") || stderr.contains("no-data"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// -s/--schema-only and --no-schema cannot be used together.
/// `pg_dumpall -s --no-schema` → error
fn pg_dumpall_schema_only_vs_no_schema() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["-s", "--no-schema"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall -s --no-schema should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema-only") || stderr.contains("no-schema"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --statistics-only and --no-statistics cannot be used together.
/// `pg_dumpall --statistics-only --no-statistics` → error
fn pg_dumpall_statistics_only_vs_no_statistics() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--statistics-only", "--no-statistics"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --statistics-only --no-statistics should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("statistics"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --statistics and --no-statistics cannot be used together.
/// `pg_dumpall --statistics --no-statistics` → error
fn pg_dumpall_statistics_vs_no_statistics() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--statistics", "--no-statistics"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --statistics --no-statistics should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("statistics"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// --statistics and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall --statistics --tablespaces-only` → error
fn pg_dumpall_statistics_vs_tablespaces_only() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--statistics", "--tablespaces-only"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --statistics --tablespaces-only should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("statistics") || stderr.contains("tablespaces-only"),
        "stderr should mention conflicting options, got: {stderr}"
    );
}

#[test]
/// Unrecognized output format is rejected.
/// `pg_dumpall --format x` → error
fn pg_dumpall_invalid_format() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--format", "x"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --format x should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("format") || stderr.contains("x"),
        "stderr should mention invalid format, got: {stderr}"
    );
}

#[test]
/// --restrict-key can only be used with --format=plain.
/// `pg_dumpall --format d --restrict-key=uu -f dumpfile` → error
fn pg_dumpall_restrict_key_requires_plain() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--format", "d", "--restrict-key=uu", "-f", "dumpfile"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --format d --restrict-key=uu should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("restrict-key") || stderr.contains("plain"),
        "stderr should mention restrict-key requires plain format, got: {stderr}"
    );
}

#[test]
/// --clean and --globals-only cannot be used together in non-text dump.
/// `pg_dumpall --format d --globals-only --clean -f dumpfile` → error
fn pg_dumpall_clean_vs_globals_only_non_text() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args([
            "--format",
            "d",
            "--globals-only",
            "--clean",
            "-f",
            "dumpfile",
        ])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --format d --globals-only --clean should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("clean") || stderr.contains("globals-only") || stderr.contains("plain"),
        "stderr should mention clean/globals-only conflict in non-plain format, got: {stderr}"
    );
}

#[test]
/// Non-plain format requires --file option.
/// `pg_dumpall --format d` → error
fn pg_dumpall_non_plain_requires_file() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dumpall"))
        .args(["--format", "d"])
        .output()
        .expect("failed to run pg_dumpall");
    assert!(
        !output.status.success(),
        "pg_dumpall --format d (no -f) should exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("file") || stderr.contains("format") || stderr.contains("plain"),
        "stderr should mention non-plain requires file, got: {stderr}"
    );
}
