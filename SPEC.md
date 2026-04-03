# SPEC.md — pg_plumbing

## Vision

pg_dump and pg_restore, rewritten in Rust. Drop-in compatible, then better.

## Phase 1: Exact Compatibility via TDD

### Goal

Pass 100% of PostgreSQL's own test suite for pg_dump and pg_restore.

### Source of truth

PostgreSQL test files:
- `src/bin/pg_dump/t/001_basic.pl`
- `src/bin/pg_dump/t/002_pg_dump.pl`
- `src/bin/pg_dump/t/003_pg_dump_with_server.pl`
- `src/bin/pg_dump/t/004_pg_dump_parallel.pl`
- `src/bin/pg_dump/t/010_dump_connstr.pl`

And pg_restore tests embedded in the same suite.

These Perl TAP tests define the expected behavior. Our job:
1. Extract each test case into a Rust integration test
2. Run it (RED — it fails because we haven't implemented the feature)
3. Implement just enough to pass (GREEN)
4. Refactor if needed
5. Move to next test

### Architecture

```
pg_plumbing/
├── Cargo.toml
├── CLAUDE.md
├── SPEC.md
├── src/
│   ├── main.rs          # CLI entry point (pg_dump / pg_restore subcommands)
│   ├── lib.rs           # Shared library
│   ├── dump/
│   │   ├── mod.rs       # pg_dump implementation
│   │   ├── format.rs    # Output formats (plain, custom, directory, tar)
│   │   ├── catalog.rs   # PostgreSQL catalog queries
│   │   └── filter.rs    # Schema/table filtering (-t, -T, -n, -N)
│   └── restore/
│       ├── mod.rs       # pg_restore implementation
│       ├── parse.rs     # Archive parsing
│       └── parallel.rs  # Parallel restore (-j)
├── tests/
│   ├── pg_dump/
│   │   ├── t001_basic.rs
│   │   ├── t002_pg_dump.rs
│   │   ├── t003_pg_dump_with_server.rs
│   │   ├── t004_pg_dump_parallel.rs
│   │   └── t010_dump_connstr.rs
│   └── pg_restore/
│       └── ...
└── .github/
    └── workflows/
        └── ci.yml       # Run tests against PG 14-18
```

### Key CLI flags to implement (pg_dump)

Priority order — most commonly used first:

1. `-F` / `--format` (plain, custom, directory, tar)
2. `-f` / `--file`
3. `-d` / `--dbname` (connection string)
4. `-t` / `--table` (include table)
5. `-T` / `--exclude-table`
6. `-n` / `--schema`
7. `-N` / `--exclude-schema`
8. `-s` / `--schema-only`
9. `-a` / `--data-only`
10. `-j` / `--jobs` (parallel dump, directory format)
11. `--no-owner`
12. `--no-privileges`
13. `--if-exists`
14. `--clean`
15. `--create`
16. `-Z` / `--compress`

### Key CLI flags to implement (pg_restore)

1. `-d` / `--dbname`
2. `-F` / `--format`
3. `-j` / `--jobs` (parallel restore)
4. `-t` / `--table`
5. `-n` / `--schema`
6. `-s` / `--schema-only`
7. `-a` / `--data-only`
8. `--no-owner`
9. `--clean`
10. `--if-exists`
11. `--create`
12. `-l` / `--list` (TOC listing)
13. `-L` / `--use-list` (selective restore from TOC)

### PG version support

14, 15, 16, 17, 18

### Sprint 1: Foundation + Basic Tests

1. **Extract test cases** — parse PostgreSQL's Perl TAP tests, document each test case as a Rust integration test stub (all RED)
2. **Scaffold project** — Cargo workspace, CI workflow, basic CLI arg parsing (clap)
3. **Implement `pg_dump --version` and `pg_dump --help`** — pass the trivial tests first
4. **Implement plain-format dump of a simple table** — `pg_dump -F plain -t tablename dbname`
5. **Implement basic `pg_restore` from custom format** — round-trip: dump → restore → verify

### Future phases (not in scope for Phase 1)

- **Performance**: parallel dump/restore faster than C pg_dump
- **Streaming**: pipe-friendly streaming output
- **Cloud-native**: direct S3/GCS upload during dump
- **Incremental**: only dump changes since last dump
- **Compression**: zstd support (beyond gzip)
- **Progress**: real-time progress bar with ETA
