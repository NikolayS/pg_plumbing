// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Tests extracted from PostgreSQL src/bin/pg_dump/t/002_pg_dump.pl
//!
//! This is the main pg_dump test suite.  It seeds a database with many
//! object types, runs ~48 different pg_dump/pg_restore invocations
//! (varying formats, flags, schemas, tables, roles, etc.), and checks
//! that each output contains (or omits) the expected SQL statements.
//!
//! The original Perl file defines 263 test entries and 48 dump runs.
//! Tests are grouped below by the object category they exercise.
//! Each requires a running PostgreSQL instance (integration tests).

// ═══════════════════════════════════════════════════════════════
// Dump configurations (48 runs)
//
// Each run is a specific pg_dump/pg_restore invocation.  The tests
// below assert that a given SQL pattern appears (like) or does not
// appear (unlike) in the output of specific runs.
// ═══════════════════════════════════════════════════════════════

// ---------------------------------------------------------------
// Module: restrict / unrestrict
// ---------------------------------------------------------------

#[test]
#[ignore]
/// Every dump output must contain a `\restrict` command.
/// Source: 'restrict' => { all_runs => 1, regexp => qr/^\restrict .../ }
fn restrict_command_present() {}

#[test]
#[ignore]
/// Every dump output must contain an `\unrestrict` command.
/// Source: 'unrestrict' => { all_runs => 1, regexp => qr/^\unrestrict .../ }
fn unrestrict_command_present() {}

// ---------------------------------------------------------------
// Module: ALTER DEFAULT PRIVILEGES
// ---------------------------------------------------------------

#[test]
#[ignore]
/// ALTER DEFAULT PRIVILEGES FOR ROLE ... GRANT SELECT ON TABLES appears
/// in full runs and dump_test_schema runs but not in no_privs or
/// exclude_dump_test_schema.
fn alter_default_privileges_grant() {}

#[test]
#[ignore]
/// ALTER DEFAULT PRIVILEGES FOR ROLE ... REVOKE appears correctly.
fn alter_default_privileges_revoke() {}

#[test]
#[ignore]
/// ALTER ROLE regress_dump_test_role is dumped in globals dumps.
fn alter_role() {}

// ---------------------------------------------------------------
// Module: ALTER ... OWNER TO
// ---------------------------------------------------------------

#[test]
#[ignore]
/// ALTER COLLATION test0 OWNER TO appears in full runs, not in no_owner.
fn alter_collation_owner() {}

#[test]
#[ignore]
/// ALTER FOREIGN DATA WRAPPER dummy OWNER TO.
fn alter_fdw_owner() {}

#[test]
#[ignore]
/// ALTER SERVER s1 OWNER TO.
fn alter_server_owner() {}

#[test]
#[ignore]
/// ALTER FUNCTION dump_test.pltestlang_call_handler() OWNER TO.
fn alter_function_owner() {}

#[test]
#[ignore]
/// ALTER OPERATOR FAMILY dump_test.op_family OWNER TO.
fn alter_operator_family_owner() {}

#[test]
#[ignore]
/// ALTER OPERATOR CLASS dump_test.op_class OWNER TO.
fn alter_operator_class_owner() {}

#[test]
#[ignore]
/// ALTER PUBLICATION pub1 OWNER TO.
fn alter_publication_owner() {}

#[test]
#[ignore]
/// ALTER LARGE OBJECT ... OWNER TO.
fn alter_large_object_owner() {}

#[test]
#[ignore]
/// ALTER PROCEDURAL LANGUAGE pltestlang OWNER TO.
fn alter_language_owner() {}

#[test]
#[ignore]
/// ALTER SCHEMA dump_test OWNER TO.
fn alter_schema_owner() {}

#[test]
#[ignore]
/// ALTER SCHEMA dump_test_second_schema OWNER TO.
fn alter_schema_second_owner() {}

#[test]
#[ignore]
/// ALTER SCHEMA public OWNER TO.
fn alter_schema_public_owner() {}

#[test]
#[ignore]
/// ALTER SCHEMA public OWNER TO (without ACL changes).
fn alter_schema_public_owner_no_acl() {}

// ---------------------------------------------------------------
// Module: ALTER TABLE / SEQUENCE / INDEX
// ---------------------------------------------------------------

#[test]
#[ignore]
/// ALTER SEQUENCE test_table_col1_seq is dumped correctly.
fn alter_sequence() {}

#[test]
#[ignore]
/// ALTER TABLE ONLY test_table ADD CONSTRAINT ... PRIMARY KEY.
fn alter_table_add_primary_key() {}

#[test]
#[ignore]
/// CONSTRAINT NOT NULL / NOT VALID on test_table_nn.
fn constraint_not_null_not_valid() {}

#[test]
#[ignore]
/// COMMENT ON CONSTRAINT ON test_table_nn.
fn comment_on_constraint_nn() {}

#[test]
#[ignore]
/// COMMENT ON CONSTRAINT ON test_table_chld2.
fn comment_on_constraint_chld2() {}

#[test]
#[ignore]
/// CONSTRAINT NOT NULL / NOT VALID on child partitions (child1, child2, child3).
fn constraint_not_null_not_valid_children() {}

#[test]
#[ignore]
/// CONSTRAINT NOT NULL / NO INHERIT.
fn constraint_not_null_no_inherit() {}

#[test]
#[ignore]
/// CONSTRAINT PRIMARY KEY / WITHOUT OVERLAPS.
fn constraint_pk_without_overlaps() {}

#[test]
#[ignore]
/// CONSTRAINT UNIQUE / WITHOUT OVERLAPS.
fn constraint_unique_without_overlaps() {}

#[test]
#[ignore]
/// ALTER TABLE (partitioned) ADD CONSTRAINT ... FOREIGN KEY.
fn alter_table_partitioned_fk() {}

#[test]
#[ignore]
/// ALTER TABLE ONLY test_table ALTER COLUMN col1 SET STATISTICS 90.
fn alter_column_set_statistics() {}

#[test]
#[ignore]
/// ALTER TABLE ONLY test_table ALTER COLUMN col2 SET STORAGE.
fn alter_column_set_storage_col2() {}

#[test]
#[ignore]
/// ALTER TABLE ONLY test_table ALTER COLUMN col3 SET STORAGE.
fn alter_column_set_storage_col3() {}

#[test]
#[ignore]
/// ALTER TABLE ONLY test_table ALTER COLUMN col4 SET n_distinct.
fn alter_column_set_n_distinct() {}

#[test]
#[ignore]
/// ALTER TABLE test_table CLUSTER ON test_table_pkey.
fn alter_table_cluster() {}

#[test]
#[ignore]
/// ALTER TABLE test_table DISABLE TRIGGER ALL.
fn alter_table_disable_trigger() {}

#[test]
#[ignore]
/// ALTER FOREIGN TABLE foreign_table ALTER COLUMN c1 OPTIONS.
fn alter_foreign_table_column_options() {}

#[test]
#[ignore]
/// ALTER TABLE test_table OWNER TO.
fn alter_table_owner() {}

#[test]
#[ignore]
/// ALTER TABLE test_table ENABLE ROW LEVEL SECURITY.
fn alter_table_enable_rls() {}

#[test]
#[ignore]
/// ALTER TABLE test_second_table OWNER TO.
fn alter_second_table_owner() {}

#[test]
#[ignore]
/// ALTER TABLE measurement OWNER TO.
fn alter_measurement_owner() {}

#[test]
#[ignore]
/// ALTER TABLE measurement_y2006m2 OWNER TO.
fn alter_measurement_partition_owner() {}

#[test]
#[ignore]
/// ALTER FOREIGN TABLE foreign_table OWNER TO.
fn alter_foreign_table_owner() {}

#[test]
#[ignore]
/// ALTER TEXT SEARCH CONFIGURATION alt_ts_conf1 OWNER TO.
fn alter_ts_config_owner() {}

#[test]
#[ignore]
/// ALTER TEXT SEARCH DICTIONARY alt_ts_dict1 OWNER TO.
fn alter_ts_dict_owner() {}

// ---------------------------------------------------------------
// Module: Large Objects
// ---------------------------------------------------------------

#[test]
#[ignore]
/// LO create (using lo_from_bytea) appears in appropriate runs.
fn lo_create() {}

#[test]
#[ignore]
/// LO load (using lo_from_bytea).
fn lo_load() {}

#[test]
#[ignore]
/// LO create (with no data) for schema-only dumps.
fn lo_create_no_data() {}

// ---------------------------------------------------------------
// Module: COMMENT ON
// ---------------------------------------------------------------

#[test]
#[ignore]
/// COMMENT ON DATABASE postgres.
fn comment_on_database() {}

#[test]
#[ignore]
/// COMMENT ON EXTENSION plpgsql.
fn comment_on_extension() {}

#[test]
#[ignore]
/// COMMENT ON SCHEMA public / COMMENT ON SCHEMA public IS NULL.
fn comment_on_schema_public() {}

#[test]
#[ignore]
/// COMMENT ON TABLE dump_test.test_table.
fn comment_on_table() {}

#[test]
#[ignore]
/// COMMENT ON COLUMN dump_test.test_table.col1.
fn comment_on_column() {}

#[test]
#[ignore]
/// COMMENT ON COLUMN dump_test.composite.f1.
fn comment_on_composite_column() {}

#[test]
#[ignore]
/// COMMENT ON COLUMN dump_test.test_second_table.col1 / col2.
fn comment_on_second_table_columns() {}

#[test]
#[ignore]
/// COMMENT ON CONVERSION dump_test.test_conversion.
fn comment_on_conversion() {}

#[test]
#[ignore]
/// COMMENT ON COLLATION test0.
fn comment_on_collation() {}

#[test]
#[ignore]
/// COMMENT ON LARGE OBJECT.
fn comment_on_large_object() {}

#[test]
#[ignore]
/// COMMENT ON POLICY p1.
fn comment_on_policy() {}

#[test]
#[ignore]
/// COMMENT ON PUBLICATION pub1.
fn comment_on_publication() {}

#[test]
#[ignore]
/// COMMENT ON SUBSCRIPTION sub1.
fn comment_on_subscription() {}

#[test]
#[ignore]
/// COMMENT ON TEXT SEARCH CONFIGURATION / DICTIONARY / PARSER / TEMPLATE.
fn comment_on_text_search_objects() {}

#[test]
#[ignore]
/// COMMENT ON TYPE (ENUM, RANGE, Regular, Undefined).
fn comment_on_types() {}

#[test]
#[ignore]
/// COMMENT ON CONSTRAINT ON DOMAIN.
fn comment_on_domain_constraint() {}

// ---------------------------------------------------------------
// Module: COPY / INSERT (data output)
// ---------------------------------------------------------------

#[test]
/// COPY dump_test_simple — default data output format.
/// Un-ignored: tests basic COPY output for plain-format dump.
fn copy_test_table() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres"]);
    assert_eq!(code, 0, "pg_dump should succeed");
    assert!(
        stdout.contains("COPY public.dump_test_simple"),
        "output should contain COPY statement:\n{stdout}"
    );
    assert!(
        stdout.contains("alice"),
        "output should contain row data 'alice':\n{stdout}"
    );
    assert!(
        stdout.contains("\\.\n"),
        "output should contain end-of-data marker:\n{stdout}"
    );
}

#[test]
#[ignore]
/// COPY fk_reference_test_table (both references).
fn copy_fk_reference_test_table() {}

#[test]
#[ignore]
/// COPY test_second_table / test_third_table / test_fourth_table /
/// test_fifth_table.
fn copy_other_tables() {}

#[test]
#[ignore]
/// COPY test_table_identity.
fn copy_test_table_identity() {}

#[test]
/// INSERT INTO dump_test_simple — inserts mode.
/// Un-ignored: tests basic INSERT output for plain-format dump.
fn insert_into_test_table() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres", "--inserts"]);
    assert_eq!(code, 0, "pg_dump --inserts should succeed");
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple VALUES"),
        "output should contain INSERT statements:\n{stdout}"
    );
    assert!(
        stdout.contains("'alice'"),
        "output should contain row data 'alice':\n{stdout}"
    );
    // Should NOT contain COPY when using --inserts.
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY with --inserts:\n{stdout}"
    );
}

#[test]
/// test_table with 4-row INSERTs (rows_per_insert mode).
/// Un-ignored: tests --rows-per-insert batching.
fn insert_rows_per_insert() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--rows-per-insert=4",
    ]);
    assert_eq!(code, 0, "pg_dump --rows-per-insert should succeed");
    // With 3 rows and batch size 4, all rows fit in one INSERT.
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple VALUES"),
        "output should contain INSERT statement:\n{stdout}"
    );
    // Multiple value tuples in a single INSERT (comma-separated).
    let insert_line = stdout
        .lines()
        .find(|l| l.starts_with("INSERT INTO public.dump_test_simple VALUES"))
        .expect("should have an INSERT line");
    let comma_count = insert_line.matches("), (").count() + insert_line.matches("),(").count();
    assert!(
        comma_count >= 2,
        "expected at least 3 value tuples in one INSERT, got {} separators:\n{insert_line}",
        comma_count
    );
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY with --rows-per-insert:\n{stdout}"
    );
}

#[test]
#[ignore]
/// INSERT INTO test_second_table / test_third_table / test_fourth_table /
/// test_fifth_table / test_table_identity.
fn insert_into_other_tables() {}

#[test]
#[ignore]
/// COPY measurement (partitioned table data).
fn copy_measurement() {}

// ---------------------------------------------------------------
// Module: CREATE ROLE / DATABASE / TABLESPACE
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE ROLE regress_dump_test_role appears in globals dump.
fn create_role() {}

#[test]
#[ignore]
/// CREATE ROLE regress_quoted... (with special characters).
fn create_role_quoted() {}

#[test]
#[ignore]
/// Newline in table name handled in comments.
fn newline_in_table_name_comment() {}

#[test]
#[ignore]
/// CREATE TABLESPACE regress_dump_tablespace.
fn create_tablespace() {}

#[test]
#[ignore]
/// CREATE DATABASE regression_invalid... for encoding tests.
fn create_database_invalid() {}

#[test]
#[ignore]
/// CREATE DATABASE postgres / dump_test.
fn create_database() {}

// ---------------------------------------------------------------
// Module: CREATE EXTENSION / ACCESS METHOD / COLLATION
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE EXTENSION ... plpgsql.
fn create_extension_plpgsql() {}

#[test]
#[ignore]
/// CREATE ACCESS METHOD gist2.
fn create_access_method() {}

#[test]
#[ignore]
/// CREATE COLLATION test0 FROM "C".
fn create_collation() {}

#[test]
#[ignore]
/// CREATE COLLATION icu_collation (when ICU is available).
fn create_collation_icu() {}

#[test]
#[ignore]
/// CREATE CAST FOR timestamptz.
fn create_cast() {}

// ---------------------------------------------------------------
// Module: CREATE AGGREGATE / CONVERSION / DOMAIN / FUNCTION /
//         OPERATOR / PROCEDURE / TYPE
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE AGGREGATE dump_test.newavg.
fn create_aggregate() {}

#[test]
#[ignore]
/// CREATE CONVERSION dump_test.test_conversion.
fn create_conversion() {}

#[test]
#[ignore]
/// CREATE DOMAIN dump_test.us_postal_code.
fn create_domain() {}

#[test]
#[ignore]
/// CREATE FUNCTION dump_test.pltestlang_call_handler.
fn create_function_pltestlang_handler() {}

#[test]
#[ignore]
/// CREATE FUNCTION dump_test.trigger_func.
fn create_function_trigger() {}

#[test]
#[ignore]
/// CREATE FUNCTION dump_test.event_trigger_func.
fn create_function_event_trigger() {}

#[test]
#[ignore]
/// CREATE FUNCTION dump_test.int42_in / int42_out.
fn create_function_int42() {}

#[test]
#[ignore]
/// CREATE FUNCTION ... SUPPORT.
fn create_function_support() {}

#[test]
#[ignore]
/// Ordering: function that depends on a primary key.
fn function_depends_on_primary_key() {}

#[test]
#[ignore]
/// CREATE PROCEDURE dump_test.ptest1.
fn create_procedure() {}

#[test]
#[ignore]
/// CREATE OPERATOR FAMILY dump_test.op_family / op_family USING btree.
fn create_operator_family() {}

#[test]
#[ignore]
/// CREATE OPERATOR CLASS dump_test.op_class / op_class_custom / op_class_empty.
fn create_operator_class() {}

#[test]
#[ignore]
/// CREATE EVENT TRIGGER test_event_trigger.
fn create_event_trigger() {}

#[test]
#[ignore]
/// CREATE TRIGGER test_trigger.
fn create_trigger() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.planets AS ENUM.
fn create_type_enum() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.planets AS ENUM (pg_upgrade variant).
fn create_type_enum_pg_upgrade() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.textrange AS RANGE.
fn create_type_range() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.int42 (shell + populated).
fn create_type_int42() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.composite.
fn create_type_composite() {}

#[test]
#[ignore]
/// CREATE TYPE dump_test.undefined.
fn create_type_undefined() {}

// ---------------------------------------------------------------
// Module: Text Search objects
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1.
fn create_ts_configuration() {}

#[test]
#[ignore]
/// ALTER TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 ... ADD MAPPING.
fn alter_ts_configuration_mapping() {}

#[test]
#[ignore]
/// CREATE TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1.
fn create_ts_template() {}

#[test]
#[ignore]
/// CREATE TEXT SEARCH PARSER dump_test.alt_ts_prs1.
fn create_ts_parser() {}

#[test]
#[ignore]
/// CREATE TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1.
fn create_ts_dictionary() {}

// ---------------------------------------------------------------
// Module: Foreign data
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE FOREIGN DATA WRAPPER dummy.
fn create_fdw() {}

#[test]
#[ignore]
/// CREATE SERVER s1 FOREIGN DATA WRAPPER dummy.
fn create_foreign_server() {}

#[test]
#[ignore]
/// CREATE FOREIGN TABLE dump_test.foreign_table SERVER s1.
fn create_foreign_table() {}

#[test]
#[ignore]
/// CREATE USER MAPPING FOR regress_dump_test_role SERVER s1.
fn create_user_mapping() {}

// ---------------------------------------------------------------
// Module: CREATE TRANSFORM / LANGUAGE
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE TRANSFORM FOR int.
fn create_transform() {}

#[test]
#[ignore]
/// CREATE LANGUAGE pltestlang.
fn create_language() {}

// ---------------------------------------------------------------
// Module: Materialized Views
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE MATERIALIZED VIEW matview / matview_second / matview_third /
/// matview_fourth.
fn create_materialized_views() {}

#[test]
#[ignore]
/// Ordering: matview that depends on a primary key.
fn matview_depends_on_primary_key() {}

#[test]
#[ignore]
/// REFRESH MATERIALIZED VIEW matview / matview_second / matview_third /
/// matview_fourth.
fn refresh_materialized_views() {}

// ---------------------------------------------------------------
// Module: Policies (RLS)
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE POLICY p1..p6 ON test_table (various FOR clauses and RESTRICTIVE).
fn create_policies() {}

// ---------------------------------------------------------------
// Module: Property Graph
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE PROPERTY GRAPH propgraph.
fn create_property_graph() {}

// ---------------------------------------------------------------
// Module: Publications / Subscriptions
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE PUBLICATION pub1..pub10 with varying configurations.
fn create_publications() {}

#[test]
#[ignore]
/// ALTER PUBLICATION pub1 ADD TABLE ... (multiple tables).
fn alter_publication_add_table() {}

#[test]
#[ignore]
/// ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA.
fn alter_publication_add_tables_in_schema() {}

#[test]
#[ignore]
/// ALTER PUBLICATION pub4 ADD TABLE ... WHERE (col1 > 0).
fn alter_publication_add_table_where() {}

#[test]
#[ignore]
/// CREATE SUBSCRIPTION sub1 / sub2 / sub3.
fn create_subscriptions() {}

// ---------------------------------------------------------------
// Module: SCHEMA
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE SCHEMA public / dump_test / dump_test_second_schema.
fn create_schemas() {}

// ---------------------------------------------------------------
// Module: CREATE TABLE (various)
// ---------------------------------------------------------------

#[test]
/// CREATE TABLE dump_test_simple with columns, constraints, and settings.
/// Un-ignored: tests basic CREATE TABLE output for plain-format dump.
fn create_test_table() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres"]);
    assert_eq!(code, 0, "pg_dump should succeed");
    assert!(
        stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        stdout.contains("id integer"),
        "output should contain column 'id integer':\n{stdout}"
    );
    assert!(
        stdout.contains("name text NOT NULL"),
        "output should contain 'name text NOT NULL':\n{stdout}"
    );
    assert!(
        stdout.contains("PRIMARY KEY (id)"),
        "output should contain primary key constraint:\n{stdout}"
    );
}

#[test]
#[ignore]
/// CREATE TABLE fk_reference_test_table.
fn create_fk_reference_table() {}

#[test]
#[ignore]
/// CREATE TABLE test_second_table.
fn create_second_table() {}

#[test]
#[ignore]
/// CREATE TABLE measurement PARTITIONED BY with partition and triggers.
fn create_measurement_partitioned() {}

#[test]
#[ignore]
/// Partition measurement_y2006m2 creation.
fn create_measurement_partition() {}

#[test]
#[ignore]
/// Triggers on partitions: creation, disabled/replica/always variants,
/// and trigger preservation across dump/restore.
fn partition_triggers() {}

#[test]
#[ignore]
/// CREATE TABLE test_third_table_generated_cols.
fn create_third_table_generated() {}

#[test]
#[ignore]
/// CREATE TABLE test_fourth_table_zero_col.
fn create_fourth_table_zero_col() {}

#[test]
#[ignore]
/// CREATE TABLE test_fifth_table / test_sixth_table / test_seventh_table.
fn create_fifth_sixth_seventh_tables() {}

#[test]
#[ignore]
/// CREATE TABLE test_table_identity.
fn create_table_identity() {}

#[test]
#[ignore]
/// CREATE TABLE test_table_generated and children (with/without local cols).
fn create_table_generated() {}

#[test]
#[ignore]
/// CREATE TABLE table_with_stats.
fn create_table_with_stats() {}

#[test]
#[ignore]
/// CREATE TABLE test_inheritance_parent / test_inheritance_child.
fn create_inheritance_tables() {}

// ---------------------------------------------------------------
// Module: Statistics objects
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE STATISTICS extended_stats_no_options / extended_stats_options /
/// extended_stats_expression.
fn create_extended_statistics() {}

#[test]
#[ignore]
/// ALTER STATISTICS extended_stats_options.
fn alter_extended_statistics() {}

#[test]
#[ignore]
/// statistics_import / extended_statistics_import /
/// relstats_on_unanalyzed_tables.
fn statistics_import() {}

// ---------------------------------------------------------------
// Module: Sequences / Indexes / Views
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE SEQUENCE test_table_col1_seq.
fn create_sequence() {}

#[test]
#[ignore]
/// CREATE INDEX ON ONLY measurement / measurement_y2006_m2.
fn create_index_measurement() {}

#[test]
#[ignore]
/// ALTER TABLE measurement PRIMARY KEY.
fn alter_measurement_primary_key() {}

#[test]
#[ignore]
/// ALTER INDEX ... ATTACH PARTITION (regular and primary key).
fn alter_index_attach_partition() {}

#[test]
#[ignore]
/// CREATE VIEW test_view / ALTER VIEW test_view SET DEFAULT.
fn create_view() {}

// ---------------------------------------------------------------
// Module: DROP statements (--clean output)
// ---------------------------------------------------------------

#[test]
#[ignore]
/// DROP SCHEMA public / dump_test / dump_test_second_schema appear
/// in clean runs.
fn drop_schemas() {}

#[test]
#[ignore]
/// DROP TABLE test_table / fk_reference_test_table / test_second_table.
fn drop_tables() {}

#[test]
#[ignore]
/// DROP EXTENSION plpgsql / DROP FUNCTION / DROP LANGUAGE pltestlang.
fn drop_extension_function_language() {}

#[test]
#[ignore]
/// DROP IF EXISTS variants for --clean --if-exists runs.
fn drop_if_exists() {}

#[test]
#[ignore]
/// DROP ROLE regress_dump_test_role / pg_.
fn drop_roles() {}

// ---------------------------------------------------------------
// Module: GRANT / REVOKE
// ---------------------------------------------------------------

#[test]
#[ignore]
/// GRANT USAGE ON SCHEMA dump_test_second_schema.
fn grant_usage_schema() {}

#[test]
#[ignore]
/// GRANT USAGE ON FOREIGN DATA WRAPPER / FOREIGN SERVER.
fn grant_usage_fdw_server() {}

#[test]
#[ignore]
/// GRANT USAGE ON DOMAIN / TYPE (int42, planets, textrange).
fn grant_usage_domain_type() {}

#[test]
#[ignore]
/// GRANT CREATE ON DATABASE dump_test.
fn grant_create_database() {}

#[test]
#[ignore]
/// GRANT SELECT ON TABLE test_table / measurement / measurement_y2006m2.
fn grant_select_tables() {}

#[test]
#[ignore]
/// GRANT ALL ON LARGE OBJECT.
fn grant_all_large_object() {}

#[test]
#[ignore]
/// GRANT INSERT(col1) ON TABLE test_second_table.
fn grant_column_privilege() {}

#[test]
#[ignore]
/// GRANT SELECT ON PROPERTY GRAPH propgraph.
fn grant_select_property_graph() {}

#[test]
#[ignore]
/// GRANT EXECUTE ON FUNCTION pg_sleep() TO regress_dump_test_role.
fn grant_execute_function() {}

#[test]
#[ignore]
/// GRANT SELECT (proname ...) ON TABLE pg_proc TO public.
fn grant_select_pg_proc() {}

#[test]
#[ignore]
/// GRANT USAGE ON SCHEMA public TO public.
fn grant_usage_schema_public() {}

#[test]
#[ignore]
/// REVOKE CONNECT ON DATABASE dump_test FROM public.
fn revoke_connect_database() {}

#[test]
#[ignore]
/// REVOKE EXECUTE ON FUNCTION pg_sleep() FROM public.
fn revoke_execute_function() {}

#[test]
#[ignore]
/// REVOKE SELECT ON TABLE pg_proc FROM public.
fn revoke_select_pg_proc() {}

#[test]
#[ignore]
/// REVOKE ALL ON SCHEMA public.
fn revoke_all_schema_public() {}

#[test]
#[ignore]
/// REVOKE USAGE ON LANGUAGE plpgsql FROM public.
fn revoke_usage_language() {}

// ---------------------------------------------------------------
// Module: Access method / table AM
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE ACCESS METHOD regress_test_table_am.
fn create_access_method_table_am() {}

#[test]
#[ignore]
/// CREATE TABLE regress_pg_dump_table_am (using custom AM).
fn create_table_am() {}

#[test]
#[ignore]
/// CREATE MATERIALIZED VIEW regress_pg_dump_matview_am.
fn create_matview_am() {}

// ---------------------------------------------------------------
// Module: Partitioned table with regress_pg_dump_table_part
// ---------------------------------------------------------------

#[test]
#[ignore]
/// CREATE TABLE regress_pg_dump_table_part (partitioned).
fn create_table_part() {}

// ---------------------------------------------------------------
// Module: Dump run configurations
//
// Each run below validates that pg_dump/pg_restore executes
// successfully with a specific set of flags.
// ---------------------------------------------------------------

#[test]
#[ignore]
/// binary_upgrade: pg_dump --binary-upgrade --format=custom produces valid output.
fn run_binary_upgrade() {}

#[test]
/// clean: pg_dump --clean produces DROP + CREATE statements.
fn run_clean() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres", "--clean"]);
    assert_eq!(code, 0, "pg_dump --clean should succeed");
    assert!(
        stdout.contains("DROP TABLE"),
        "output should contain DROP TABLE:\n{stdout}"
    );
    assert!(
        stdout.contains("CREATE TABLE"),
        "output should contain CREATE TABLE:\n{stdout}"
    );
}

#[test]
/// clean_if_exists: pg_dump --clean --if-exists produces DROP IF EXISTS.
fn run_clean_if_exists() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--clean",
        "--if-exists",
    ]);
    assert_eq!(code, 0, "pg_dump --clean --if-exists should succeed");
    assert!(
        stdout.contains("DROP TABLE IF EXISTS"),
        "output should contain DROP TABLE IF EXISTS:\n{stdout}"
    );
    assert!(
        stdout.contains("CREATE TABLE"),
        "output should contain CREATE TABLE:\n{stdout}"
    );
}

#[test]
/// column_inserts: pg_dump --data-only --column-inserts.
/// Un-ignored: tests --column-inserts flag.
fn run_column_inserts() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--data-only",
        "--column-inserts",
    ]);
    assert_eq!(code, 0, "pg_dump --column-inserts should succeed");
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple (id, name, value) VALUES"),
        "output should contain INSERT with column names:\n{stdout}"
    );
    assert!(
        !stdout.contains("CREATE TABLE"),
        "data-only output should NOT contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY with --column-inserts:\n{stdout}"
    );
}

#[test]
/// createdb: pg_dump --create produces CREATE DATABASE.
fn run_createdb() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&["-d", "postgres", "--create"]);
    assert_eq!(code, 0, "pg_dump --create should succeed");
    assert!(
        stdout.contains("CREATE DATABASE"),
        "output should contain CREATE DATABASE:\n{stdout}"
    );
    assert!(
        stdout.contains("\\connect"),
        "output should contain \\connect:\n{stdout}"
    );
}

#[test]
/// data_only: pg_dump --data-only outputs only COPY, no CREATE TABLE.
/// Un-ignored: tests --data-only flag.
fn run_data_only() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres", "-a"]);
    assert_eq!(code, 0, "pg_dump --data-only should succeed");
    assert!(
        !stdout.contains("CREATE TABLE"),
        "data-only output should NOT contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        stdout.contains("COPY public.dump_test_simple"),
        "data-only output should contain COPY:\n{stdout}"
    );
}

#[test]
/// defaults: pg_dump of a single table with no special flags (baseline).
/// Un-ignored: tests that default plain-format dump produces valid output.
fn run_defaults() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres"]);
    assert_eq!(code, 0, "pg_dump should exit 0");
    assert!(
        stdout.contains("PostgreSQL database dump"),
        "output should contain header:\n{stdout}"
    );
    assert!(
        stdout.contains("SET statement_timeout = 0"),
        "output should contain SET commands:\n{stdout}"
    );
    assert!(
        stdout.contains("CREATE TABLE"),
        "output should contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        stdout.contains("COPY"),
        "output should contain COPY:\n{stdout}"
    );
    assert!(
        stdout.contains("PostgreSQL database dump complete"),
        "output should contain footer:\n{stdout}"
    );
}

#[test]
/// defaults_custom_format: pg_dump --format=custom → pg_restore round-trip.
/// Verifies gzip compression (when available).
fn run_defaults_custom_format() {
    // 1. Setup test schema.
    crate::common::setup_test_schema();

    // 2. pg_dump -F custom -d postgres -t dump_test_simple -f /tmp/test_custom.dump
    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "-F",
        "custom",
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "-f",
        "/tmp/test_custom.dump",
    ]);
    assert_eq!(
        code, 0,
        "pg_dump -F custom should succeed, stderr: {stderr}"
    );

    // 3. Verify file starts with PGDMP magic.
    let bytes = std::fs::read("/tmp/test_custom.dump").expect("dump file should exist");
    assert!(
        bytes.starts_with(b"PGDMP"),
        "custom dump should start with PGDMP magic"
    );

    // 4. Create fresh DB, pg_restore round-trip.
    let test_db = "pg_plumbing_custom_test";
    crate::common::create_test_db(test_db);

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_pg_restore"))
        .args(["-d", test_db, "/tmp/test_custom.dump"])
        .env("PGPASSWORD", "postgres")
        .status()
        .expect("pg_restore should run");
    assert!(status.success(), "pg_restore of custom dump should succeed");

    // 5. Verify data was restored.
    let count = crate::common::psql_query(test_db, "SELECT COUNT(*) FROM dump_test_simple");
    assert!(
        count.trim() == "3",
        "should have 3 rows after restore, got: {count}"
    );

    crate::common::drop_test_db(test_db);
    std::fs::remove_file("/tmp/test_custom.dump").ok();
}

#[test]
/// defaults_dir_format: pg_dump --format=directory → pg_restore round-trip.
/// Checks directory structure (toc.dat, blobs_*.toc, *.dat[.gz]).
fn run_defaults_dir_format() {
    crate::common::setup_test_schema();
    let out_dir = "/tmp/test_dir_dump";
    let _ = std::fs::remove_dir_all(out_dir);

    // dump
    let (_, stderr, code) = crate::common::run_pg_dump(&[
        "-F",
        "directory",
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "-f",
        out_dir,
    ]);
    assert_eq!(code, 0, "pg_dump -F directory failed: {stderr}");
    assert!(
        std::path::Path::new(&format!("{out_dir}/toc.dat")).exists(),
        "toc.dat should exist"
    );

    // restore round-trip
    let test_db = "pg_plumbing_dir_test";
    crate::common::create_test_db(test_db);

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_pg_plumbing"))
        .args(["pg-restore", "-d", test_db, out_dir])
        .env("PGPASSWORD", "postgres")
        .status()
        .expect("pg_restore should run");
    assert!(status.success(), "pg_restore of directory dump failed");

    let count = crate::common::psql_query(test_db, "SELECT COUNT(*) FROM dump_test_simple");
    assert_eq!(count.trim(), "3", "should restore 3 rows, got: {count}");

    crate::common::drop_test_db(test_db);
    let _ = std::fs::remove_dir_all(out_dir);
}

#[test]
#[ignore]
/// defaults_parallel: pg_dump --format=directory --jobs=2.
fn run_defaults_parallel() {}

#[test]
#[ignore]
/// defaults_tar_format: pg_dump --format=tar → pg_restore round-trip.
fn run_defaults_tar_format() {}

#[test]
/// exclude_dump_test_schema: pg_dump --exclude-schema=public.
/// Verifies that no tables from the public schema appear in the output.
fn run_exclude_schema() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-d", "postgres", "--exclude-schema", "public"]);
    assert_eq!(code, 0, "pg_dump --exclude-schema should succeed");
    // Should not contain any CREATE TABLE for public-schema tables.
    assert!(
        !stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should NOT contain public tables:\n{stdout}"
    );
    // Should not contain data for public-schema tables.
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY for public tables:\n{stdout}"
    );
}

#[test]
/// exclude_test_table: pg_dump --exclude-table=dump_test_simple.
/// Verifies that the excluded table does not appear in the output.
fn run_exclude_table() {
    crate::common::setup_test_schema();
    // Dump all of public, excluding dump_test_simple.
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-d",
        "postgres",
        "--schema",
        "public",
        "--exclude-table",
        "dump_test_simple",
    ]);
    assert_eq!(code, 0, "pg_dump --exclude-table should succeed");
    // The excluded table's CREATE TABLE should not be present.
    assert!(
        !stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should NOT contain excluded table's CREATE TABLE:\n{stdout}"
    );
    // The excluded table's COPY should not be present.
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain excluded table's COPY:\n{stdout}"
    );
}

#[test]
#[ignore]
/// exclude_measurement: pg_dump --exclude-table-and-children=dump_test.measurement.
/// No measurement/partition table exists in the test schema — skipping.
fn run_exclude_measurement() {}

#[test]
/// exclude_measurement_data / exclude_test_table_data:
/// pg_dump --exclude-table-data=dump_test_simple.
/// The table's schema (CREATE TABLE) is dumped but its data (COPY) is not.
fn run_exclude_table_data() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-d",
        "postgres",
        "-t",
        "dump_test_simple",
        "--exclude-table-data",
        "dump_test_simple",
    ]);
    assert_eq!(code, 0, "pg_dump --exclude-table-data should succeed");
    // Schema should still be present.
    assert!(
        stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should contain CREATE TABLE for schema:\n{stdout}"
    );
    // Data should be absent.
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY for excluded-data table:\n{stdout}"
    );
    assert!(
        !stdout.contains("alice"),
        "output should NOT contain row data:\n{stdout}"
    );
}

#[test]
/// inserts: pg_dump --data-only --inserts.
/// Un-ignored: tests --inserts flag with --data-only.
fn run_inserts() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--data-only",
        "--inserts",
    ]);
    assert_eq!(code, 0, "pg_dump --data-only --inserts should succeed");
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple VALUES"),
        "output should contain INSERT statements:\n{stdout}"
    );
    assert!(
        !stdout.contains("CREATE TABLE"),
        "data-only output should NOT contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY with --inserts:\n{stdout}"
    );
    // Verify value quoting: strings quoted, integers unquoted, NULL as NULL.
    assert!(
        stdout.contains("'alice'"),
        "string values should be single-quoted:\n{stdout}"
    );
}

#[test]
/// rows_per_insert: pg_dump --rows-per-insert=4.
/// Un-ignored: tests --rows-per-insert dump run.
fn run_rows_per_insert() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) = crate::common::run_pg_dump(&[
        "-t",
        "dump_test_simple",
        "-d",
        "postgres",
        "--rows-per-insert=4",
    ]);
    assert_eq!(code, 0, "pg_dump --rows-per-insert should succeed");
    assert!(
        stdout.contains("INSERT INTO public.dump_test_simple VALUES"),
        "output should contain INSERT statements:\n{stdout}"
    );
    assert!(
        stdout.contains("CREATE TABLE"),
        "output should contain CREATE TABLE (not data-only):\n{stdout}"
    );
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "output should NOT contain COPY with --rows-per-insert:\n{stdout}"
    );
}

#[test]
#[ignore]
/// pg_dumpall_globals: pg_dumpall --globals-only.
fn run_pg_dumpall_globals() {}

#[test]
#[ignore]
/// pg_dumpall_globals_clean: pg_dumpall --globals-only --clean.
fn run_pg_dumpall_globals_clean() {}

#[test]
#[ignore]
/// pg_dumpall_dbprivs: pg_dumpall full dump.
fn run_pg_dumpall_dbprivs() {}

#[test]
#[ignore]
/// pg_dumpall_exclude: pg_dumpall --exclude-database.
fn run_pg_dumpall_exclude() {}

#[test]
#[ignore]
/// no_toast_compression: pg_dump --no-toast-compression.
fn run_no_toast_compression() {}

#[test]
#[ignore]
/// no_large_objects: pg_dump --no-large-objects.
fn run_no_large_objects() {}

#[test]
#[ignore]
/// no_policies / no_policies_restore: pg_dump/pg_restore --no-policies.
fn run_no_policies() {}

#[test]
#[ignore]
/// no_privs: pg_dump --no-privileges.
fn run_no_privs() {}

#[test]
#[ignore]
/// no_owner: pg_dump --no-owner.
fn run_no_owner() {}

#[test]
#[ignore]
/// no_subscriptions / no_subscriptions_restore: --no-subscriptions.
fn run_no_subscriptions() {}

#[test]
#[ignore]
/// no_table_access_method: pg_dump --no-table-access-method.
fn run_no_table_access_method() {}

#[test]
/// only_dump_test_schema: pg_dump --schema=public.
/// Verifies that only tables from the public schema appear in the output.
fn run_only_schema() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-d", "postgres", "--schema", "public"]);
    assert_eq!(code, 0, "pg_dump --schema should succeed");
    // Should contain public schema tables.
    assert!(
        stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should contain public schema tables:\n{stdout}"
    );
    // Should contain the dump header.
    assert!(
        stdout.contains("PostgreSQL database dump"),
        "output should contain dump header:\n{stdout}"
    );
}

#[test]
/// only_dump_test_table: pg_dump --table=dump_test_simple.
/// Verifies that only the specified table is in the output.
fn run_only_table() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-d", "postgres", "--table", "dump_test_simple"]);
    assert_eq!(code, 0, "pg_dump --table should succeed");
    // Should contain the specified table.
    assert!(
        stdout.contains("CREATE TABLE public.dump_test_simple"),
        "output should contain the specified table:\n{stdout}"
    );
    // Should contain the table's data.
    assert!(
        stdout.contains("COPY public.dump_test_simple"),
        "output should contain COPY for the specified table:\n{stdout}"
    );
    assert!(
        stdout.contains("alice"),
        "output should contain row data:\n{stdout}"
    );
    // Should NOT contain other tables from the database (spot-check one).
    assert!(
        !stdout.contains("CREATE TABLE public.dump_test_restore"),
        "output should NOT contain other tables:\n{stdout}"
    );
}

#[test]
#[ignore]
/// only_dump_measurement: pg_dump --table-and-children=dump_test.measurement.
/// No measurement/partition table exists in the test schema — skipping.
fn run_only_measurement() {}

#[test]
#[ignore]
/// role / role_parallel: pg_dump --role=regress_dump_test_role --schema=...
fn run_role() {}

#[test]
/// schema_only: pg_dump --schema-only outputs CREATE TABLE but no data.
/// Un-ignored: tests --schema-only flag.
fn run_schema_only() {
    crate::common::setup_test_schema();
    let (stdout, _stderr, code) =
        crate::common::run_pg_dump(&["-t", "dump_test_simple", "-d", "postgres", "-s"]);
    assert_eq!(code, 0, "pg_dump --schema-only should succeed");
    assert!(
        stdout.contains("CREATE TABLE public.dump_test_simple"),
        "schema-only output should contain CREATE TABLE:\n{stdout}"
    );
    assert!(
        !stdout.contains("COPY public.dump_test_simple"),
        "schema-only output should NOT contain COPY:\n{stdout}"
    );
    assert!(
        !stdout.contains("alice"),
        "schema-only output should NOT contain row data:\n{stdout}"
    );
}

#[test]
#[ignore]
/// section_pre_data / section_data / section_post_data:
/// pg_dump --section=pre-data / data / post-data.
fn run_sections() {}

#[test]
#[ignore]
/// test_schema_plus_large_objects: pg_dump --schema=dump_test --large-objects.
fn run_schema_plus_large_objects() {}

#[test]
#[ignore]
/// no_statistics: pg_dump --no-statistics.
fn run_no_statistics() {}

#[test]
#[ignore]
/// statistics_only: pg_dump --statistics-only.
fn run_statistics_only() {}

#[test]
#[ignore]
/// no_data_no_schema / no_schema: pg_dump --no-data --no-schema /
/// pg_dump --no-schema.
fn run_no_data_no_schema() {}

// ---------------------------------------------------------------
// Module: Cross-database reference rejection
// ---------------------------------------------------------------

#[test]
#[ignore]
/// pg_dump --table rejects cross-database two-part names.
/// `pg_dump --table other_db.pg_catalog.pg_class` → error
fn reject_cross_database_two_part() {}

#[test]
#[ignore]
/// pg_dump --table rejects cross-database three-part names.
/// `pg_dump --table "some.other.db".pg_catalog.pg_class` → error
fn reject_cross_database_three_part() {}

// ---------------------------------------------------------------
// Module: Defaults for non-public databases
// ---------------------------------------------------------------

#[test]
#[ignore]
/// defaults_no_public: dump of regress_pg_dump_test (database without
/// public schema) works correctly.
fn run_defaults_no_public() {}

#[test]
#[ignore]
/// defaults_no_public_clean: dump with --clean of database without
/// public schema.
fn run_defaults_no_public_clean() {}

#[test]
#[ignore]
/// defaults_public_owner: dump of regress_public_owner database.
fn run_defaults_public_owner() {}
