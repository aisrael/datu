# datu Version Notes

## v0.3.4

### Highlights

- **REPL and pipeline**: Aggregate functions (`avg`, `min`, `max`) with validation and clearer selection and error handling.
- **XLSX**: Refactored writing to use newer Arrow array APIs.
- **Quality**: Parquet validation helper; REPL and conversion tests refactored for clarity.
- **Docs**: Pipeline diagram and documentation layout updates.

### Improvements

- **Pipeline**
  - Unified dispatch for construction; continued modularization after the v0.3.3 pipeline work.

- **REPL**
  - Aggregate support and incremental test cleanup.

- **Tooling**
  - Release workflow docs updated so `make-release` can run on `release/*` branches.

### Changelog Stats

- 18 commits
- 53 files changed
- 5528 insertions
- 5252 deletions

## v0.3.3

### Highlights

- **Pipeline**: Large refactor toward a unified CLI and REPL pipeline—shared `ReadArgs`, consolidated readers, and `heads_or_tails` handling.
- **Writes**: Replaced per-format write markers with a single `WriteResult` type for clearer sink outcomes.
- **Planning errors**: Added `PipelinePlanningError::ConflictingOptions` when pipeline options are mutually exclusive.

### Improvements

- **CLI and REPL**
  - Shared read path and arguments between interactive and command-line use.

- **Pipeline internals**
  - Unified write completion signaling via `WriteResult`.
  - Stricter validation for conflicting pipeline options.

### Changelog Stats

- 4 commits
- 46 files changed
- 4714 insertions
- 2643 deletions

## v0.3.2

### Highlights

- **Convert command** now uses the DataFrame API for streamlined file processing; internal `resolve_input_file_type` renamed to `resolve_file_type`.
- **Avro**: Added compatibility for Int16 fields in record batches.
- **Error handling**: Replaced `anyhow` with `eyre`.
- **REPL**: Refactored evaluation to use `exec_*` naming and improved test structure; feature files updated with REPL equivalents.
- **Docs**: README updates for JSON support and REPL usage.
- **CI**: Switched to `actions-rust-lang/setup-rust-toolchain@v1`, Rust 1.94.0, no nightly.

### Improvements

- **Convert**
  - Implemented DataFrame API support for file processing in the convert command.

- **Avro pipeline**
  - Int16 fields in record batches are now handled correctly when writing Avro.

- **REPL**
  - Evaluation methods refactored to `exec_*` naming convention.
  - Test structure and REPL feature scenarios updated.

- **Project**
  - Added `.clocignore`.
  - Dependency and version updates in Cargo files.
  - CI workflow and toolchain adjustments.

### Changelog Stats

- 16 commits
- 28 files changed
- 754 insertions
- 610 deletions

## v0.3.1

### Highlights

- Added a `sample` CLI command and REPL function for randomly sampling rows from data files.
- Replaced the `spinoff` spinner with an `indicatif` progress bar in the `convert` command, showing row-level progress for Parquet and ORC inputs.
- Improved REPL with incremental execution and persistent history across sessions.
- Internal refactoring of record-batch writing and tail slicing into shared pipeline helpers.

### New Features

- **`sample` command (CLI and REPL)**
  - `datu sample <FILE> [-n N]` — print N random rows from a Parquet, Avro, CSV, or ORC file (default: 10).
  - Uses index-based sampling for Parquet and ORC (metadata-aware); reservoir sampling for Avro and CSV.
  - Supports `--select`, `--output`, `--sparse`, and `--has-headers` options (same as `head`/`tail`).
  - REPL: `read("file") |> sample(5)` — samples 5 random rows from the pipeline.

- **Progress bar for `convert`**
  - Parquet and ORC inputs show a row-level progress bar (using file metadata for total row count).
  - Avro and CSV inputs show an animated spinner (total row count not available upfront).

### Improvements

- **REPL enhancements**
  - Incremental pipeline execution: stages are evaluated as they are parsed.
  - Command history is now persisted across sessions.

- **Internal refactoring**
  - Extracted record-batch writing into a shared sink harness (`batch_write` module).
  - Extracted tail batch slicing into a shared pipeline helper.

### Changelog Stats

- 7 commits
- 20 files changed
- 1317 insertions
- 157 deletions

## v0.3.0

### Highlights

- Added an interactive REPL mode: running `datu` without a subcommand now starts an interactive pipeline shell.
- Added CSV input support across key commands: `convert`, `count`, `schema`, `head`, and `tail`.
- Refactored the pipeline engine toward an async/DataFusion DataFrame-based architecture for cleaner composition and execution.
- Expanded test coverage with new REPL feature tests and reorganized CLI feature tests.

### New Features

- **Interactive REPL**
  - New REPL entrypoint and parser/evaluator flow.
  - Supports pipeline-style expressions (for example: `read("input.parquet") |> select(:id, :email) |> write("output.csv")`).
  - Added REPL integration tests and feature scenarios.

- **CSV as a first-class input format**
  - CSV can now be read by:
    - `datu convert`
    - `datu count`
    - `datu schema`
    - `datu head`
    - `datu tail`
  - Added `--has-headers[=bool]` to control CSV header handling.

### Improvements

- **Pipeline refactor**
  - Introduced and reworked modules for datasource and I/O concerns (`dataframe`, `datasource`, `read`, `write`, `select`).
  - Async pipeline execution improvements and clearer separation of responsibilities.

- **Documentation**
  - README expanded with command usage updates, CSV examples/options, and REPL usage examples.

- **Project metadata**
  - Added MIT `LICENSE`.
  - Updated CI/config and dependencies for new runtime/testing capabilities.

### Breaking / Behavioral Changes

- Running `datu` with no subcommand now launches the REPL.
- Commands that now accept CSV input may behave differently based on `--has-headers` (defaults to `true` when omitted).

### Testing

- CLI feature scenarios reorganized under `features/cli/*`.
- New REPL feature files and integration tests (`tests/repl.rs`).

### Changelog Stats

- 10 commits
- 46 files changed
- 5374 insertions
- 698 deletions
