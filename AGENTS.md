# AGENTS.md

## General Guidelines

- Keep changes scoped to the user request and avoid unrelated refactors.
- Never revert user-authored changes unless explicitly requested.
- When generating commit messages, if the staged changes are limited to one file, keep the commit message to one line.
- After a major change or refactoring, run `cargo +nightly fmt`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`.
- In any Git worktree, the final validation step must include both `cargo clippy` and `cargo test`.

## Rust Code Guidelines

- Use one `use` statement per line. Do not use grouped imports.
- Prefer named arguments in `println!()` when practical.
- Run formatting with `cargo +nightly fmt` (not stable `cargo fmt`).

## Testing Guidance

- Prefer targeted test commands during development to keep iteration fast.
- Before finalizing work, run the required repository-wide validation commands from the repo root.
- Treat warnings surfaced by clippy as actionable unless there is a documented reason not to.

## Cursor Cloud Specific Instructions

- Work only on the assigned feature branch unless the user explicitly asks to switch.
- Create small, logical commits and push after each completed iteration.
- Avoid force-push unless explicitly requested.
