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
#[ignore] // RED — not yet implemented
/// pg_dump --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_dump')
fn pg_dump_help() {}

#[test]
#[ignore]
/// pg_dump --version exits 0 and prints version string.
/// Source: program_version_ok('pg_dump')
fn pg_dump_version() {}

#[test]
#[ignore]
/// pg_dump rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_dump')
fn pg_dump_options_handling() {}

#[test]
#[ignore]
/// pg_restore --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_restore')
fn pg_restore_help() {}

#[test]
#[ignore]
/// pg_restore --version exits 0 and prints version string.
/// Source: program_version_ok('pg_restore')
fn pg_restore_version() {}

#[test]
#[ignore]
/// pg_restore rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_restore')
fn pg_restore_options_handling() {}

#[test]
#[ignore]
/// pg_dumpall --help exits 0 and prints usage info.
/// Source: program_help_ok('pg_dumpall')
fn pg_dumpall_help() {}

#[test]
#[ignore]
/// pg_dumpall --version exits 0 and prints version string.
/// Source: program_version_ok('pg_dumpall')
fn pg_dumpall_version() {}

#[test]
#[ignore]
/// pg_dumpall rejects unknown options with nonzero exit.
/// Source: program_options_handling_ok('pg_dumpall')
fn pg_dumpall_options_handling() {}

// ---------------------------------------------------------------
// Invalid option combinations — pg_dump
// ---------------------------------------------------------------

#[test]
#[ignore]
/// pg_dump errors on too many command-line arguments.
/// `pg_dump qqq abc` → error: too many command-line arguments (first is "abc")
fn pg_dump_too_many_args() {}

#[test]
#[ignore]
/// -s/--schema-only and -a/--data-only cannot be used together.
/// `pg_dump -s -a` → error
fn pg_dump_schema_only_vs_data_only() {}

#[test]
#[ignore]
/// -s/--schema-only and --statistics-only cannot be used together.
/// `pg_dump -s --statistics-only` → error
fn pg_dump_schema_only_vs_statistics_only() {}

#[test]
#[ignore]
/// -a/--data-only and --statistics-only cannot be used together.
/// `pg_dump -a --statistics-only` → error
fn pg_dump_data_only_vs_statistics_only() {}

#[test]
#[ignore]
/// --include-foreign-data and -s/--schema-only cannot be used together.
/// `pg_dump -s --include-foreign-data=xxx` → error
fn pg_dump_foreign_data_vs_schema_only() {}

#[test]
#[ignore]
/// --statistics-only and --no-statistics cannot be used together.
/// `pg_dump --statistics-only --no-statistics` → error
fn pg_dump_statistics_only_vs_no_statistics() {}

#[test]
#[ignore]
/// --include-foreign-data is not supported with parallel backup.
/// `pg_dump -j2 --include-foreign-data=xxx` → error
fn pg_dump_foreign_data_vs_parallel() {}

#[test]
#[ignore]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_dump -c -a` → error
fn pg_dump_clean_vs_data_only() {}

#[test]
#[ignore]
/// --if-exists requires -c/--clean.
/// `pg_dump --if-exists` → error
fn pg_dump_if_exists_requires_clean() {}

#[test]
#[ignore]
/// Parallel backup only supported by directory format.
/// `pg_dump -j3` → error (default format is plain, not directory)
fn pg_dump_parallel_requires_directory() {}

#[test]
#[ignore]
/// -j/--jobs must be in range (rejects negative values).
/// `pg_dump -j -1` → error
fn pg_dump_jobs_must_be_in_range() {}

#[test]
#[ignore]
/// Invalid output format is rejected.
/// `pg_dump -F garbage` → error: invalid output format
fn pg_dump_invalid_format() {}

#[test]
#[ignore]
/// Unrecognized compression algorithm is rejected.
/// `pg_dump --compress garbage` → error
fn pg_dump_invalid_compress_algorithm() {}

#[test]
#[ignore]
/// Compression algorithm "none" does not accept a compression level.
/// `pg_dump --compress none:1` → error
fn pg_dump_compress_none_with_level() {}

#[test]
#[ignore]
/// gzip compression level must be in valid range (1-9).
/// `pg_dump -Z 15` → error (requires libz)
fn pg_dump_gzip_level_out_of_range() {}

#[test]
#[ignore]
/// Compression is not supported by tar archive format.
/// `pg_dump --compress 1 --format tar` → error (requires libz)
fn pg_dump_compress_vs_tar() {}

#[test]
#[ignore]
/// Non-integer compression option is rejected.
/// `pg_dump -Z gzip:nonInt` → error (requires libz)
fn pg_dump_compress_non_integer() {}

#[test]
#[ignore]
/// --extra-float-digits must be in range.
/// `pg_dump --extra-float-digits -16` → error
fn pg_dump_extra_float_digits_range() {}

#[test]
#[ignore]
/// --rows-per-insert must be in range (rejects 0).
/// `pg_dump --rows-per-insert 0` → error
fn pg_dump_rows_per_insert_range() {}

#[test]
#[ignore]
/// --on-conflict-do-nothing requires --inserts, --rows-per-insert,
/// or --column-inserts.
/// `pg_dump --on-conflict-do-nothing` → error
fn pg_dump_on_conflict_requires_inserts() {}

// ---------------------------------------------------------------
// Invalid option combinations — pg_restore
// ---------------------------------------------------------------

#[test]
#[ignore]
/// pg_restore errors on too many command-line arguments.
/// `pg_restore qqq abc` → error: too many command-line arguments
fn pg_restore_too_many_args() {}

#[test]
#[ignore]
/// pg_restore requires one of -d/--dbname and -f/--file.
/// `pg_restore` (no args) → error
fn pg_restore_requires_dbname_or_file() {}

#[test]
#[ignore]
/// -a/--data-only and -s/--schema-only cannot be used together.
/// `pg_restore -s -a -f -` → error
fn pg_restore_data_only_vs_schema_only() {}

#[test]
#[ignore]
/// -d/--dbname and -f/--file cannot be used together.
/// `pg_restore -d xxx -f xxx` → error
fn pg_restore_dbname_vs_file() {}

#[test]
#[ignore]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_restore -c -a -f -` → error
fn pg_restore_clean_vs_data_only() {}

#[test]
#[ignore]
/// --if-exists requires -c/--clean.
/// `pg_restore --if-exists -f -` → error
fn pg_restore_if_exists_requires_clean() {}

#[test]
#[ignore]
/// -j/--jobs must be in range (rejects negative values).
/// `pg_restore -j -1 -f -` → error
fn pg_restore_jobs_must_be_in_range() {}

#[test]
#[ignore]
/// Cannot specify both --single-transaction and multiple jobs.
/// `pg_restore --single-transaction -j3 -f -` → error
fn pg_restore_single_transaction_vs_parallel() {}

#[test]
#[ignore]
/// Unrecognized archive format is rejected.
/// `pg_restore -f - -F garbage` → error
fn pg_restore_invalid_format() {}

#[test]
#[ignore]
/// Empty archive format string is rejected.
/// `pg_restore -f - -F ""` → error
fn pg_restore_empty_format() {}

#[test]
#[ignore]
/// -C/--create and -1/--single-transaction cannot be used together.
/// `pg_restore -C -1 -f -` → error
fn pg_restore_create_vs_single_transaction() {}

#[test]
#[ignore]
/// --exclude-database and -g/--globals-only cannot be used together.
/// `pg_restore --exclude-database=foo --globals-only -d xxx` → error
fn pg_restore_exclude_database_vs_globals_only() {}

#[test]
#[ignore]
/// -a/--data-only and -g/--globals-only cannot be used together.
/// `pg_restore --data-only --globals-only -d xxx` → error
fn pg_restore_data_only_vs_globals_only() {}

#[test]
#[ignore]
/// -g/--globals-only and -s/--schema-only cannot be used together.
/// `pg_restore --schema-only --globals-only -d xxx` → error
fn pg_restore_globals_only_vs_schema_only() {}

#[test]
#[ignore]
/// -g/--globals-only and --statistics-only cannot be used together.
/// `pg_restore --statistics-only --globals-only -d xxx` → error
fn pg_restore_globals_only_vs_statistics_only() {}

#[test]
#[ignore]
/// --exclude-database can only be used with pg_dumpall archives.
/// `pg_restore --exclude-database=foo -d xxx dumpdir` → error
fn pg_restore_exclude_database_requires_dumpall_archive() {}

#[test]
#[ignore]
/// --globals-only can only be used with pg_dumpall archives.
/// `pg_restore --globals-only -d xxx dumpdir` → error
fn pg_restore_globals_only_requires_dumpall_archive() {}

#[test]
#[ignore]
/// --globals-only and --no-globals cannot be used together.
/// `pg_restore --globals-only --no-globals -d xxx dumpdir` → error
fn pg_restore_globals_only_vs_no_globals() {}

// ---------------------------------------------------------------
// Invalid option combinations — pg_dumpall
// ---------------------------------------------------------------

#[test]
#[ignore]
/// pg_dumpall errors on too many command-line arguments.
/// `pg_dumpall qqq abc` → error
fn pg_dumpall_too_many_args() {}

#[test]
#[ignore]
/// -c/--clean and -a/--data-only cannot be used together.
/// `pg_dumpall -c -a` → error
fn pg_dumpall_clean_vs_data_only() {}

#[test]
#[ignore]
/// -g/--globals-only and -r/--roles-only cannot be used together.
/// `pg_dumpall -g -r` → error
fn pg_dumpall_globals_only_vs_roles_only() {}

#[test]
#[ignore]
/// -g/--globals-only and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall -g -t` → error
fn pg_dumpall_globals_only_vs_tablespaces_only() {}

#[test]
#[ignore]
/// -r/--roles-only and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall -r -t` → error
fn pg_dumpall_roles_only_vs_tablespaces_only() {}

#[test]
#[ignore]
/// --if-exists requires -c/--clean.
/// `pg_dumpall --if-exists` → error
fn pg_dumpall_if_exists_requires_clean() {}

#[test]
#[ignore]
/// --exclude-database and -g/--globals-only cannot be used together.
/// `pg_dumpall --exclude-database=foo --globals-only` → error
fn pg_dumpall_exclude_database_vs_globals_only() {}

#[test]
#[ignore]
/// -a/--data-only and --no-data cannot be used together.
/// `pg_dumpall -a --no-data` → error
fn pg_dumpall_data_only_vs_no_data() {}

#[test]
#[ignore]
/// -s/--schema-only and --no-schema cannot be used together.
/// `pg_dumpall -s --no-schema` → error
fn pg_dumpall_schema_only_vs_no_schema() {}

#[test]
#[ignore]
/// --statistics-only and --no-statistics cannot be used together.
/// `pg_dumpall --statistics-only --no-statistics` → error
fn pg_dumpall_statistics_only_vs_no_statistics() {}

#[test]
#[ignore]
/// --statistics and --no-statistics cannot be used together.
/// `pg_dumpall --statistics --no-statistics` → error
fn pg_dumpall_statistics_vs_no_statistics() {}

#[test]
#[ignore]
/// --statistics and -t/--tablespaces-only cannot be used together.
/// `pg_dumpall --statistics --tablespaces-only` → error
fn pg_dumpall_statistics_vs_tablespaces_only() {}

#[test]
#[ignore]
/// Unrecognized output format is rejected.
/// `pg_dumpall --format x` → error
fn pg_dumpall_invalid_format() {}

#[test]
#[ignore]
/// --restrict-key can only be used with --format=plain.
/// `pg_dumpall --format d --restrict-key=uu -f dumpfile` → error
fn pg_dumpall_restrict_key_requires_plain() {}

#[test]
#[ignore]
/// --clean and --globals-only cannot be used together in non-text dump.
/// `pg_dumpall --format d --globals-only --clean -f dumpfile` → error
fn pg_dumpall_clean_vs_globals_only_non_text() {}

#[test]
#[ignore]
/// Non-plain format requires --file option.
/// `pg_dumpall --format d` → error
fn pg_dumpall_non_plain_requires_file() {}
