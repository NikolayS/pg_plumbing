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

/// Set up the issue-50 test schema: matviews, triggers, event triggers,
/// procedures, transforms, extended statistics, and type comments.
/// Uses OnceLock to avoid poisoning.
pub fn setup_issue50_schema() {
    use std::sync::OnceLock;
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        // We need the base schema first (provides dump_test_simple with PK).
        setup_test_schema();

        let conninfo = test_conninfo("postgres");
        let password = std::env::var("PGPASSWORD").unwrap_or_default();

        // Step 1: base table for trigger tests + matviews
        let sql1 = "\
            CREATE TABLE IF NOT EXISTS test_table (\
                col1 int PRIMARY KEY,\
                col2 text\
            );\
            CREATE TABLE IF NOT EXISTS test_table_part (\
                col1 int,\
                col2 text\
            );\
        ";
        // Step 2: trigger function + trigger + disabled trigger
        let sql2 = "\
            CREATE OR REPLACE FUNCTION public.trigger_func() RETURNS trigger LANGUAGE plpgsql AS \
            $$ BEGIN RETURN NEW; END; $$;\
            DROP TRIGGER IF EXISTS test_trigger ON test_table;\
            CREATE TRIGGER test_trigger BEFORE INSERT OR UPDATE ON test_table \
                FOR EACH ROW EXECUTE FUNCTION public.trigger_func();\
            DROP TRIGGER IF EXISTS test_trigger_disabled ON test_table_part;\
            CREATE TRIGGER test_trigger_disabled BEFORE INSERT ON test_table_part \
                FOR EACH ROW EXECUTE FUNCTION public.trigger_func();\
            ALTER TABLE test_table_part DISABLE TRIGGER ALL;\
        ";
        // Step 3: event trigger function + event trigger
        let sql3 = "\
            CREATE OR REPLACE FUNCTION public.event_trigger_func() RETURNS event_trigger \
            LANGUAGE plpgsql AS $$ BEGIN END; $$;\
            DROP EVENT TRIGGER IF EXISTS test_event_trigger;\
            CREATE EVENT TRIGGER test_event_trigger ON ddl_command_start \
                EXECUTE FUNCTION public.event_trigger_func();\
        ";
        // Step 4: procedure
        let sql4 = "\
            CREATE OR REPLACE PROCEDURE public.ptest1(a int) LANGUAGE plpgsql AS \
            $$ BEGIN RAISE NOTICE '%', a; END; $$;\
        ";
        // Step 5: materialized views
        let sql5 = "\
            DROP MATERIALIZED VIEW IF EXISTS public.matview CASCADE;\
            DROP MATERIALIZED VIEW IF EXISTS public.matview_second CASCADE;\
            CREATE MATERIALIZED VIEW public.matview AS \
                SELECT id, name FROM dump_test_simple;\
            CREATE MATERIALIZED VIEW public.matview_second AS \
                SELECT id FROM dump_test_simple WHERE value > 1;\
        ";
        // Step 6: extended statistics
        let sql6 = "\
            DROP STATISTICS IF EXISTS public.extended_stats_options;\
            CREATE STATISTICS public.extended_stats_options (dependencies) \
                ON id, value FROM dump_test_simple;\
            ALTER STATISTICS public.extended_stats_options SET STATISTICS 100;\
        ";
        // Step 7: type comments (create a custom enum type)
        let sql7 = "\
            DROP TYPE IF EXISTS public.test_enum_type CASCADE;\
            CREATE TYPE public.test_enum_type AS ENUM ('alpha', 'beta', 'gamma');\
            COMMENT ON TYPE public.test_enum_type IS 'test enum type for pg_plumbing';\
        ";
        // Step 8: transform (via hstore + hstore_plpython3u extension)
        // This is optional: plpython3u may not be installed in all CI environments.
        let sql8 = "\
            CREATE EXTENSION IF NOT EXISTS hstore;\
            CREATE EXTENSION IF NOT EXISTS plpython3u;\
            CREATE EXTENSION IF NOT EXISTS hstore_plpython3u;\
        ";

        // Steps 1-7 are required; step 8 (transform/plpython3u) is best-effort.
        for (step, sql) in [sql1, sql2, sql3, sql4, sql5, sql6, sql7]
            .iter()
            .enumerate()
        {
            let mut cmd = Command::new("psql");
            cmd.arg(&conninfo).arg("-c").arg(sql);
            if !password.is_empty() {
                cmd.env("PGPASSWORD", &password);
            }
            let output = cmd.output().expect("psql setup_issue50 failed");
            assert!(
                output.status.success(),
                "setup_issue50_schema step {} failed: {}",
                step + 1,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Step 8: best-effort – skip silently if plpython3u is unavailable.
        {
            let mut cmd = Command::new("psql");
            cmd.arg(&conninfo).arg("-c").arg(sql8);
            if !password.is_empty() {
                cmd.env("PGPASSWORD", &password);
            }
            let _ = cmd.output(); // ignore errors
        }
    });
}
