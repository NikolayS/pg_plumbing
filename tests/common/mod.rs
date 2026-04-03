// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Shared test helpers for pg_dump integration tests.

use std::process::Command;

/// Return the path to the compiled pg_plumbing binary.
pub fn pg_plumbing_bin() -> String {
    // cargo test sets CARGO_BIN_EXE_pg_plumbing when using [[bin]] targets,
    // but for integration tests we find it via cargo_bin.
    let mut path = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("parent")
        .parent()
        .expect("grandparent")
        .to_path_buf();
    path.push("pg_plumbing");
    path.to_string_lossy().to_string()
}

/// Build a connection string from environment variables.
pub fn test_conninfo(dbname: &str) -> String {
    let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
    let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("PGPASSWORD").unwrap_or_default();
    if password.is_empty() {
        format!("host={host} port={port} user={user} dbname={dbname}")
    } else {
        format!("host={host} port={port} user={user} password={password} dbname={dbname}")
    }
}

/// Run pg_plumbing pg-dump with the given arguments.
/// Returns (stdout, stderr, exit_code).
pub fn run_pg_dump(args: &[&str]) -> (String, String, i32) {
    let bin = pg_plumbing_bin();
    let mut cmd = Command::new(&bin);
    cmd.arg("pg-dump");
    cmd.args(args);
    let output = cmd.output().expect("failed to execute pg_plumbing");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let code = output.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

/// Set up the test database schema. Idempotent.
/// Uses OnceLock to avoid Once poisoning when setup fails.
pub fn setup_test_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "\
            drop table if exists dump_test_simple cascade;\n\
            drop sequence if exists dump_test_simple_id_seq cascade;\n\
            create table dump_test_simple (\n\
                id serial primary key,\n\
                name text not null,\n\
                value integer\n\
            );\n\
            insert into dump_test_simple (name, value) values\n\
                ('alice', 1),\n\
                ('bob', 2),\n\
                ('charlie', 3);\n\
        ";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup failed");
        assert!(
            output.status.success(),
            "setup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}
