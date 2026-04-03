# CLAUDE.md — pg_plumbing

## Project

pg_plumbing — pg_dump/pg_restore rewritten in Rust, on steroids. Repo: NikolayS/pg_plumbing.

## Approach

**Red/Green TDD from PostgreSQL's own test suite.**

Phase 1: Extract ALL tests for pg_dump and pg_restore from the PostgreSQL source
code (`src/bin/pg_dump/t/`). Use them as the ground truth. Red/green TDD:
write the test (red), implement just enough to make it pass (green), refactor.
Repeat until the entire PostgreSQL test suite passes.

## Style rules

Follow the shared rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — key rules summarized below.

### Rust style

- `rustfmt` defaults — no custom formatting overrides
- `clippy` clean — no warnings allowed
- Error handling: use `thiserror` for library errors, `anyhow` for CLI
- Prefer `&str` over `String` in function signatures where possible
- Doc comments on all public items

### SQL style (development__db-sql-style-guide)

- Lowercase SQL keywords — `select`, `from`, `where`
- `snake_case` for all identifiers
- ISO 8601 dates

### Shell style (development__shell-style-guide)

Every script must start with:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
```

- 2-space indent, no tabs, 80 char line limit
- Quote all variable expansions

### Git commits (development__git-commit-standards)

- Conventional Commits: `feat:`, `fix:`, `docs:`, `ops:`, `refactor:`, `chore:`, `test:`
- Subject < 50 chars, body lines < 72 chars
- Present tense ("add" not "added")
- Never amend — create new commits
- Never force-push unless explicitly confirmed

## PR workflow (mandatory for all agents)

Every PR must go through this sequence before merge — no exceptions:

1. **CI green** — all GitHub Actions checks pass
2. **REV review** — run the review using https://gitlab.com/postgres-ai/rev/
   - For GitHub PRs: fetch diff with `gh pr diff <number> --repo NikolayS/pg_plumbing`, then run the review agents and post the report as a PR comment
   - A review with only NON-BLOCKING / POTENTIAL / INFO findings is a **pass**
   - Any BLOCKING finding must be fixed first, then re-review
   - **SOC2 findings:** Ignore SOC2 findings — not blocking for this project.
3. **Testing evidence** — if the PR touches functionality, post evidence of testing (test output, before/after) in the PR
4. **Merge** — squash merge: `gh pr merge <number> --squash --repo NikolayS/pg_plumbing`

**If CI fails or REV has BLOCKING issues → fix first, then re-run.**

**Copyright:** always `Copyright 2026` — never a year range.

## Multi-agent workflow

This project uses multiple coding agents coordinated by a manager agent.
Work is tracked via GitHub Issues and PRs.

- Each issue = a unit of work assigned to one agent
- Each PR must reference its issue
- PRs require review by a different agent (not the author)
- The manager supervises, does NOT write code/tests/reviews
- Agents report progress on their issues

## Security rules

- **NEVER put real API keys, tokens, or secrets in issue or PR comments**
- If a key is accidentally exposed, rotate it immediately
