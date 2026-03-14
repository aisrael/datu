# datu Version Notes

## v0.3.1

Compare range: `0.3.0...0.3.1`

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

Compare range: `0.2.4...0.3.0`

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
