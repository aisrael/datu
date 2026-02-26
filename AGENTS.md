# General Guidelines

- After a major change or refactoring, run `cargo +nightly fmt` and `cargo test` to ensure linting and tests pass

# Rust Code Guidelines

- When writing imports, use one `use` statement per line. DO NOT use grouped `use` statements
- When writing `println!()` statements, use named arguments whenever possible.
- When running `cargo fmt`, use `cargo +nightly fmt`
