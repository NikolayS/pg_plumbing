// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Shared test helpers for pg_dump/pg_restore integration tests.

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

/// Run pg_plumbing pg-restore with the given arguments.
/// Returns (stdout, stderr, exit_code).
pub fn run_pg_restore(args: &[&str]) -> (String, String, i32) {
    let bin = pg_plumbing_bin();
    let mut cmd = Command::new(&bin);
    cmd.arg("pg-restore");
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
            COMMENT ON TABLE dump_test_simple IS 'test table for pg_plumbing';\n\
            COMMENT ON COLUMN dump_test_simple.name IS 'person name column';\n\
            COMMENT ON SCHEMA public IS 'standard public schema';\n\
            GRANT SELECT ON TABLE dump_test_simple TO PUBLIC;\n\
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

/// Run a SQL command via psql against the given database.
/// Panics on failure.
pub fn psql(dbname: &str, sql: &str) {
    let conninfo = test_conninfo(dbname);
    let password = std::env::var("PGPASSWORD").unwrap_or_default();
    let mut cmd = Command::new("psql");
    cmd.arg(&conninfo).arg("-c").arg(sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", &password);
    }
    let output = cmd.output().expect("psql failed");
    assert!(
        output.status.success(),
        "psql command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Run a SQL query via psql and return the output (tuples-only, no alignment).
pub fn psql_query(dbname: &str, sql: &str) -> String {
    let conninfo = test_conninfo(dbname);
    let password = std::env::var("PGPASSWORD").unwrap_or_default();
    let mut cmd = Command::new("psql");
    cmd.arg(&conninfo)
        .arg("-tA") // tuples only, unaligned
        .arg("-c")
        .arg(sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", &password);
    }
    let output = cmd.output().expect("psql query failed");
    assert!(
        output.status.success(),
        "psql query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Create a fresh test database. Drops it first if it exists.
pub fn create_test_db(dbname: &str) {
    drop_test_db(dbname);

    let conninfo = test_conninfo("postgres");
    let password = std::env::var("PGPASSWORD").unwrap_or_default();
    let create_sql = format!("create database \"{dbname}\";");
    let mut cmd = Command::new("psql");
    cmd.arg(&conninfo).arg("-c").arg(&create_sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", &password);
    }
    let output = cmd.output().expect("create database failed");
    assert!(
        output.status.success(),
        "create database failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Set up a simple restore-test table (no SERIAL/sequence dependency).
/// Uses OnceLock to avoid poisoning.
pub fn setup_restore_test_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "\
            drop table if exists dump_test_restore cascade;\n\
            create table dump_test_restore (\n\
                id integer not null,\n\
                name text not null,\n\
                value integer,\n\
                constraint dump_test_restore_pkey primary key (id)\n\
            );\n\
            insert into dump_test_restore (id, name, value) values\n\
                (1, 'alice', 10),\n\
                (2, 'bob', 20),\n\
                (3, 'charlie', 30);\n\
        ";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup_restore failed");
        assert!(
            output.status.success(),
            "setup_restore failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

/// Drop a test database if it exists.
pub fn drop_test_db(dbname: &str) {
    let conninfo = test_conninfo("postgres");
    let password = std::env::var("PGPASSWORD").unwrap_or_default();

    // Terminate connections first.
    let term_sql = format!(
        "select pg_terminate_backend(pid) \
         from pg_stat_activity \
         where datname = '{dbname}' and pid <> pg_backend_pid();"
    );
    let mut cmd = Command::new("psql");
    cmd.arg(&conninfo).arg("-c").arg(&term_sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", &password);
    }
    let _ = cmd.output();

    // Now drop.
    let drop_sql = format!("drop database if exists \"{dbname}\";");
    let mut cmd = Command::new("psql");
    cmd.arg(&conninfo).arg("-c").arg(&drop_sql);
    if !password.is_empty() {
        cmd.env("PGPASSWORD", &password);
    }
    let _ = cmd.output();
}

/// Set up the view test schema. Requires setup_test_schema() to have run first.
/// Uses OnceLock to avoid poisoning.
pub fn setup_view_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "CREATE OR REPLACE VIEW dump_test_view AS \
                   SELECT id, name FROM dump_test_simple;";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup_view failed");
        assert!(
            output.status.success(),
            "setup_view failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

/// Set up the dump_test schema with multiple test tables.
/// Uses OnceLock to avoid poisoning.
pub fn setup_dump_test_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "\
            CREATE SCHEMA IF NOT EXISTS dump_test;\n\
            DROP TABLE IF EXISTS dump_test.test_inheritance_child CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_inheritance_parent CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_second_table CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_fourth_table_zero_col CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_fifth_table CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_sixth_table CASCADE;\n\
            DROP TABLE IF EXISTS dump_test.test_seventh_table CASCADE;\n\
            CREATE TABLE dump_test.test_second_table (\n\
                id integer,\n\
                col1 text,\n\
                col2 text\n\
            );\n\
            INSERT INTO dump_test.test_second_table VALUES\n\
                (1, 'foo', 'bar'),\n\
                (2, 'baz', 'qux');\n\
            CREATE TABLE dump_test.test_fourth_table_zero_col ();\n\
            CREATE TABLE dump_test.test_fifth_table (\n\
                id integer,\n\
                val text\n\
            );\n\
            CREATE TABLE dump_test.test_sixth_table (\n\
                id integer,\n\
                val text\n\
            );\n\
            CREATE TABLE dump_test.test_seventh_table (\n\
                id integer,\n\
                val text\n\
            );\n\
            CREATE TABLE dump_test.test_inheritance_parent (\n\
                id integer PRIMARY KEY,\n\
                val text\n\
            );\n\
            CREATE TABLE dump_test.test_inheritance_child (\n\
                extra_col integer\n\
            ) INHERITS (dump_test.test_inheritance_parent);\n\
            ALTER SCHEMA dump_test OWNER TO postgres;\n\
        ";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup_dump_test_schema failed");
        assert!(
            output.status.success(),
            "setup_dump_test_schema failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

/// Set up the dump_test_second_schema (empty schema).
/// Uses OnceLock to avoid poisoning.
pub fn setup_dump_test_second_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "\
            CREATE SCHEMA IF NOT EXISTS dump_test_second_schema;\n\
            ALTER SCHEMA dump_test_second_schema OWNER TO postgres;\n\
        ";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup_dump_test_second_schema failed");
        assert!(
            output.status.success(),
            "setup_dump_test_second_schema failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}

/// Set up the parallel-dump test schema (partitioned tables + enum type).
/// Uses OnceLock to avoid poisoning.
pub fn setup_parallel_test_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();
        let sql = "\
            drop table if exists tht cascade;\
            drop table if exists ths cascade;\
            drop table if exists tplain cascade;\
            drop type if exists digit cascade;\
            create type digit as enum ('0','1','2','3','4','5','6','7','8','9');\
            create table tplain (en digit, data int unique);\
            insert into tplain select '0'::digit, generate_series(1,100);\
            create table ths (mod int, data int) partition by hash(mod);\
            create table ths_0 partition of ths for values with (modulus 3, remainder 0);\
            create table ths_1 partition of ths for values with (modulus 3, remainder 1);\
            create table ths_2 partition of ths for values with (modulus 3, remainder 2);\
            insert into ths select mod(i,100), i from generate_series(1,300) i;\
            create table tht (en digit, data int) partition by hash(en);\
            create table tht_0 partition of tht for values with (modulus 3, remainder 0);\
            create table tht_1 partition of tht for values with (modulus 3, remainder 1);\
            create table tht_2 partition of tht for values with (modulus 3, remainder 2);\
            insert into tht select (mod(i,10)::text)::digit, i from generate_series(1,300) i;\
        ";
        let mut cmd = Command::new("psql");
        cmd.arg(&conninfo).arg("-c").arg(sql);
        if !password.is_empty() {
            cmd.env("PGPASSWORD", &password);
        }
        let output = cmd.output().expect("psql setup_parallel failed");
        assert!(
            output.status.success(),
            "setup_parallel_test_schema failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    });
}
