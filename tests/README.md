# pg_plumbing — Test Suite

Tests are extracted from the PostgreSQL source tree and translated into
Rust integration test stubs.  Every stub is `#[ignore]` (RED) until the
corresponding feature is implemented.

## Source mapping

| Rust test file | PostgreSQL source | What it covers |
|---|---|---|
| `pg_dump/t001_basic.rs` | `src/bin/pg_dump/t/001_basic.pl` (354 lines) | CLI basics: `--help`, `--version`, invalid option detection, mutually exclusive flag combinations for pg_dump, pg_restore, and pg_dumpall. No PG instance needed. |
| `pg_dump/t002_pg_dump.rs` | `src/bin/pg_dump/t/002_pg_dump.pl` (5342 lines) | Main test suite: 48 dump/restore configurations × 263 pattern checks covering every object type (tables, schemas, views, types, functions, triggers, policies, publications, subscriptions, statistics, etc.). Requires PG instance. |
| `pg_dump/t003_pg_dump_with_server.rs` | `src/bin/pg_dump/t/003_pg_dump_with_server.pl` (50 lines) | Foreign data wrapper dumping: `--include-foreign-data` with dummy FDW, empty-server edge case. Requires PG instance. |
| `pg_dump/t004_pg_dump_parallel.rs` | `src/bin/pg_dump/t/004_pg_dump_parallel.pl` (90 lines) | Parallel dump/restore (`--jobs`) with plain tables and hash-partitioned tables (including enum partition keys). Requires PG instance. |
| `pg_dump/t010_dump_connstr.rs` | `src/bin/pg_dump/t/010_dump_connstr.pl` (295 lines) | Connection strings with LATIN1 special characters in database and user names; parallel dump/restore with special names; full dump → psql restore via env vars and command-line options. Requires PG instance. |

## Running tests

```bash
# Run all tests including ignored (RED) stubs
cargo test -- --ignored

# Run only a specific test file
cargo test --test pg_dump_tests pg_dump::t001_basic -- --ignored

# Count test stubs
cargo test -- --ignored --list 2>/dev/null | grep -c 'test$'
```

## Test progression

The TDD workflow:

1. Pick an `#[ignore]` test → understand what it checks
2. Implement the feature in `src/`
3. Remove `#[ignore]` → run the test → it should pass (GREEN)
4. Refactor if needed
5. Commit and move to the next test

## PostgreSQL source reference

Tests were extracted from:
<https://github.com/postgres/postgres/tree/master/src/bin/pg_dump/t>

The original Perl TAP tests use `PostgreSQL::Test::Cluster` and
`PostgreSQL::Test::Utils` to start a PG instance, seed it with objects,
run pg_dump/pg_restore, and check the output against regexps.
