# General Guidelines

- When working in a Git worktree, as a final step make sure `cargo clippy` and `cargo test` both pass.
- When generating commit messages, if the staged changes are limited to one file, keep the commit message to one line.
- After a major change or refactoring, run `cargo +nightly fmt`, `cargo clippy --all-targets -- -D warnings` and `cargo test` to ensure linting and tests pass

# Rust Code Guidelines

- When writing imports, use one `use` statement per line. DO NOT use grouped `use` statements
- When writing `println!()` statements, use named arguments whenever possible.
- When running `cargo fmt`, use `cargo +nightly fmt`

## Cursor Cloud specific instructions

- **Toolchain**: `rust-toolchain.toml` pins to nightly. The VM snapshot already has nightly installed; `cargo build` will use it automatically.
- **No external services**: This is a self-contained CLI tool with zero database, Docker, or network dependencies. All data I/O is local files.
- **Lint / Test / Build commands** (see also `AGENTS.md` General Guidelines and `.github/workflows/ci.yml`):
  - Format check: `cargo +nightly fmt -- --check`
  - Clippy: `cargo clippy --all-targets -- -D warnings`
  - Tests: `cargo test` (runs 129 unit tests, 12 integration tests, and 65 Cucumber BDD scenarios)
  - Build: `cargo build`
  - Run: `cargo run -- <subcommand>` (e.g. `cargo run -- schema fixtures/userdata.parquet`)
- **BDD integration tests** use Cucumber/Gherkin feature files in `features/cli/` and `features/repl/`, with test harnesses in `tests/features.rs` and `tests/repl.rs`. The REPL tests use `expectrl` to drive an interactive session, so they require a PTY-capable environment.
- **Fixture data** lives in `fixtures/` and is checked into the repo (Parquet, Avro, ORC, CSV files).
