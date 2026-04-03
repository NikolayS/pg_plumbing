# pg_plumbing

pg_dump and pg_restore, rewritten in Rust. Drop-in compatible, then better.

## Status

Early development. See [SPEC.md](SPEC.md) for the roadmap.

## Building

```bash
cargo build
```

## Usage

```bash
# Show version
pg_plumbing pg-dump --version

# Show help
pg_plumbing pg-dump --help
pg_plumbing pg-restore --help
```

## Testing

```bash
cargo test
```

Requires a running PostgreSQL instance for integration tests.

## License

MIT
