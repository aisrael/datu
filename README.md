datu - a data file utility
=======================

> *Datu* (Filipino) - a traditional chief or local leader

`datu` is intended to be a lightweight, fast, and versatile CLI tool for reading, querying, and converting data in various file formats, such as Parquet, Avro, ORC, CSV, JSON, YAML, and .XLSX.

## Installation

**Prerequisites:** Rust ~> 1.95 (or recent stable)

```sh
cargo install datu
```

To install from source:

```sh
cargo install --git https://github.com/aisrael/datu
```

## Supported Formats

| Format                        | Read | Write | Display |
|-------------------------------|:----:|:-----:|:-------:|
| Parquet (`.parquet`, `.parq`) |  ✓   |   ✓   |    —    |
| Avro (`.avro`)                |  ✓   |   ✓   |    —    |
| ORC (`.orc`)                  |  ✓   |   ✓   |    —    |
| CSV (`.csv`)                  |  ✓   |   ✓   |    ✓    |
| JSON (`.json`)                |  ✓   |   ✓   |    ✓    |
| XLSX (`.xlsx`)                |  —   |   ✓   |    —    |
| JSON (pretty)                 |  —   |   —   |    ✓    |
| YAML                          |  —   |   —   |    ✓    |

- **Read** — Input file formats for `concat`, `convert`, `count`, `schema`, `head`, `tail`, and `split`.
- **Write** — Output file formats for `concat`, `convert`, and `split`.
- **Display** — Output format when printing to stdout (`schema`, `head`, `tail` via `--output`: csv, json, json-pretty, yaml).

**File type detection:** By default, file types are inferred from extensions. Use `--input <TYPE>` (`-I`) to override input format detection, and `--output <TYPE>` (`-O`, `concat`, `convert`, and `split` only) to override output format detection. Valid types: `avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`.

**CSV options:** When reading CSV files, the `--input-headers` option controls whether the first row is treated as column names. Omitted or `--input-headers` means true (header present); `--input-headers=false` for headerless CSV. Applies to `concat`, `convert`, `count`, `schema`, `head`, `tail`, and `split`.

Usage
=====

`datu` can be used non-interactively as a typical command-line utility, _or_ it can be ran without specifying a command in interactive mode, providing a REPL-like interface.

For example, the command

```sh
datu convert table.parquet --select id,email table.csv
```

And, interactively, using the REPL

```sh
datu
> read("table.parquet") |> select(:id, :email) |> write("table.csv")
```

Perform the same conversion and column filtering.

## Commands

### `concat`

Concatenate two or more input files into a single output file. The last positional argument is the output file; every argument before it is an input file path or shell-style glob pattern (e.g. `part*.avro`). Input and output formats are inferred from file extensions, or can be specified explicitly with `--input` and `--output`. All inputs must have a compatible (union-able) schema.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), JSON (`.json`), ORC (`.orc`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage (CLI):**

```sh
datu concat <INPUT>... <OUTPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection for every input. |
| `-O`, `--output <TYPE>` | Output file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values (e.g. empty string). |
| `--json-pretty` | When concatenating to JSON, format output with indentation and newlines. Ignored for other output formats. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# Concatenate explicit files into one Parquet file
datu concat part0.avro part1.avro part3.avro all.parquet

# Concatenate using a glob pattern
datu concat "part*.avro" all.parquet

# Mix literal paths and globs
datu concat 2024-*.csv late-arrival.csv all-2024.csv

# Concatenate Parquet files into a single CSV
datu concat batch1.parquet batch2.parquet combined.csv
```

---

### `split`

Split a single input file into multiple output files of at most `--split` rows each — the inverse of `concat`. Partition files are named by inserting a zero-padded `.partNNNNN` segment (1-based) before the extension of the output path, which defaults to the input path (so partitions land next to the input file). Input and output formats are inferred from file extensions, or can be specified explicitly with `--input` and `--output`.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), JSON (`.json`), ORC (`.orc`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage (CLI):**

```sh
datu split <INPUT> [OUTPUT] [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-O`, `--output <TYPE>` | Output file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--split <N>` | Maximum size of each output partition: a row count (e.g. `100000`), or a byte size with a unit — `kb`/`mb`/`gb`/`tb` (decimal, base 1000) or `kib`/`mib`/`gib`/`tib` (binary, base 1024), case-insensitive (e.g. `64mb`, `1.5GiB`). Byte sizes are approximate, estimated from in-memory row size rather than the exact on-disk/compressed size. Default: `100000` rows. |
| `--limit <N>` | Maximum number of total rows to process across all partitions. Default: `0` (unlimited). |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values (e.g. empty string). |
| `--json-pretty` | When splitting to JSON, format output with indentation and newlines. Ignored for other output formats. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# Split into 100000-row partitions next to the input file (large-file.part00001.avro, ...)
datu split large-file.avro

# Split into 50000-row partitions
datu split large-file.avro --split 50000

# Split into a specific output directory/base name (out/data.part00001.csv, ...)
datu split large-file.avro out/data.csv --split 50000

# Split only the first 10000 rows, in partitions of 2000
datu split large-file.parquet --split 2000 --limit 10000

# Split Avro into Parquet partitions
datu split large-file.avro out.parquet --split 100000

# Split into ~64MB partitions instead of a row count
datu split large-file.avro --split 64mb

# Split into ~1.5GiB partitions (binary unit)
datu split large-file.parquet --split 1.5GiB
```

---

### `convert`

Convert data between supported formats. Input and output formats are inferred from file extensions, or can be specified explicitly with `--input` and `--output`.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), JSON (`.json`), ORC (`.orc`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage (CLI):**

```sh
datu convert <INPUT> <OUTPUT> [OPTIONS]
```

**Usage (REPL):**

```flt
read("table.parquet") |> select(:id, :email) |> write("table.csv")
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-O`, `--output <TYPE>` | Output file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are written. Column names can be given as multiple arguments or as comma-separated values (e.g. `--select id,name,email` or `--select id --select name --select email`). |
| `--limit <N>` | Maximum number of records to read from the input. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values (e.g. empty string). |
| `--json-pretty` | When converting to JSON, format output with indentation and newlines. Ignored for other output formats. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# Parquet to CSV (all columns)
datu convert data.parquet data.csv

# CSV to Parquet (with automatic type inference)
datu convert data.csv data.parquet

# Parquet to Avro (first 1000 rows)
datu convert data.parquet data.avro --limit 1000

# Avro to CSV, only specific columns
datu convert events.avro events.csv --select id,timestamp,user_id

# CSV to JSON with headerless input
datu convert data.csv output.json --input-headers=false

# Parquet to Parquet with column subset
datu convert input.parq output.parquet --select one,two,three

# JSON to CSV or Parquet
datu convert data.json data.csv
datu convert data.json data.parquet

# Parquet, Avro, CSV, JSON, or ORC to Excel (.xlsx)
datu convert data.parquet report.xlsx

# Parquet, Avro, or JSON to ORC
datu convert data.parquet data.orc

# Parquet, Avro, or JSON to JSON
datu convert data.parquet data.json
```

---

### `schema`

Display the schema of a Parquet, Avro, CSV, or ORC file (column names, types, and nullability). Useful for inspecting file structure without reading data. CSV schema uses type inference from the data.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Usage (CLI):**

```sh
datu schema <FILE> [OPTIONS]
```

**Usage (REPL):**

```flt
read("file") |> schema()
```

Use `schema()` after a read to print the schema of the data in the pipeline. For a single file, `read("data.parquet") |> schema()` is equivalent.

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Output formats:**

- **csv** (default): One line per column, e.g. `name: String (UTF8), nullable`.
- **json**: JSON array of objects with `name`, `data_type`, `nullable`, and optionally `converted_type` (Parquet).
- **json-pretty**: Same as `json` but pretty-printed for readability.
- **yaml**: YAML list of mappings with the same fields.

**Examples:**

```sh
# Default CSV-style output
datu schema data.parquet

# JSON output
datu schema data.parquet --output json

# JSON pretty-printed
datu schema data.parquet --output json-pretty

# YAML output (e.g. for config or tooling)
datu schema events.avro --output yaml
datu schema events.avro -o YAML
```

---

### `count`

Return the number of rows in a Parquet, Avro, CSV, or ORC file.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Usage (CLI):**

```sh
datu count <FILE> [OPTIONS]
```

**Usage (REPL):**

```flt
count("file")
```

Count rows in a file directly. Or use `read("file") |> count()`

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# Count rows in a Parquet file
datu count data.parquet

# Count rows in an Avro, CSV, or ORC file
datu count events.avro
datu count data.csv
datu count data.orc

# Count rows in a headerless CSV file
datu count data.csv --input-headers=false
```

---

### `diff`

Compare two data files of the same format row-by-row. Reports whether the files are identical, or lists rows unique to each file.

Schemas are compared first. If the files have columns that differ (by name or type), the differing columns are printed before the row comparison proceeds over the common columns only. If no common columns exist, the command exits with an error.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), JSON (`.json`), ORC (`.orc`).

**Usage (CLI):**

```sh
datu diff <FILE1> <FILE2> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`). Applied to both files; overrides extension-based detection. |
| `--limit <N>` | Maximum total differing rows to display (file1 + file2 combined). Default: `100`. Use `0` for unlimited. |
| `--json` | Emit the diff result as a single JSON document on stdout instead of the human-readable text output. |
| `-o`, `--output <FORMAT>` | Output format. Currently only `json` is accepted; equivalent to `--json`. |

**Output:**

- If both files are identical over all common columns: `Files are identical (N rows)`.
- Otherwise, rows present only in `FILE1` and rows present only in `FILE2` are each printed in tab-separated format with a header line of column names.
- Schema-only differences (columns not shared between both files) are reported on stderr before the row comparison output.

**JSON output (`--json` / `--output json`):**

When JSON output is requested, a single JSON document is written to stdout (schema differences are included in the document rather than printed to stderr). The shape depends on whether the files match:

- **Identical files:** `identical` is `true` and `row_count` reports the number of compared rows.
- **Differing files:** `identical` is `false`; `columns` lists the common columns, optional `schema` reports columns unique to each file, and `only_in_file1` / `only_in_file2` hold the differing rows as objects keyed by column name. `truncated` and `limit` are included when the scan stopped early.

> **Note on `--limit`:** The two files are streamed and compared row-by-row, and the scan stops early as soon as the running diff count reaches the limit — without reading either file to the end (except JSON, which is buffered fully before comparison). Because the comparison is incomplete when it stops early, some reported rows may be **false positives**: a row counted as unique to one file might have been cancelled out by a matching row later in the other file. Use `--limit 0` to disable the limit and get an exact, complete comparison.
>
> **Note on `--limit 0`:** Disabling the limit forces both files to be read in full and every distinct row to be held in memory. On extremely large inputs this can take a very long time and/or exhaust available memory. Prefer a bounded `--limit` value when working with large files.

**Examples:**

```sh
# Compare two Avro files
datu diff old.avro new.avro

# Compare two CSV files
datu diff snapshot.csv current.csv

# Force format detection for files without standard extensions
datu diff old_data new_data --input parquet

# Emit the diff result as JSON
datu diff old.avro new.avro --json
```

**Example output** (files differ in schema and rows):

```text
Schema differences:
  Only in new.avro:
    email: Utf8
Only in old.avro (1 row):
  id    name
  1     foo

Only in new.avro (1 row):
  id    name
  4     fizz
```

> Row values are tab-separated. Schema differences are printed to stderr; row differences are printed to stdout.

**Example JSON output** (`--json`, same inputs):

```json
{
  "identical": false,
  "file1": "old.avro",
  "file2": "new.avro",
  "columns": ["id", "name"],
  "schema": {
    "only_in_file2": [{ "name": "email", "data_type": "Utf8" }]
  },
  "only_in_file1": [{ "id": "1", "name": "foo" }],
  "only_in_file2": [{ "id": "4", "name": "fizz" }],
  "limit": 100
}
```

---

### `sample`

Print N randomly sampled rows from a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats), or write them to a file by supplying an optional `OUTPUT` path. For Parquet and ORC, sampling uses file metadata to determine total row count and selects random indices; for Avro and CSV, reservoir sampling is used.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Supported output formats (when writing to a file):** CSV (`.csv`), JSON (`.json`), YAML (`.yaml`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage (CLI):**

```sh
datu sample <INPUT> [OUTPUT] [OPTIONS]
```

When `OUTPUT` is omitted, rows are printed to stdout and `--output` selects the display format. When `OUTPUT` is provided, rows are written to that file and the format is inferred from its extension (or `-O`/`--output-type`).

**Usage (REPL):**

```flt
read("file") |> sample(n)
```

Prints _n_ random rows to stdout. Chain `|> write("out.csv")` to write to a file, e.g. `read("data.parquet") |> sample(5) |> write("sample.csv")`.

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to sample. Default: 10. |
| `--output <FORMAT>` | Display format when printing to stdout: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. Cannot be combined with an `OUTPUT` file. |
| `-O`, `--output-type <TYPE>` | Output file type when writing to a file (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--json-pretty` | When writing JSON to a file, format output with indentation and newlines. Ignored for other output formats. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# 10 random rows (default)
datu sample data.parquet

# 5 random rows
datu sample data.parquet -n 5
datu sample data.avro --number 5
datu sample data.csv -n 5
datu sample data.orc --number 5

# 20 random rows, specific columns
datu sample data.parquet -n 20 --select id,name,email

# Write a random sample to a file (format from extension)
datu sample input.parquet output.avro -n 5
datu sample data.parquet sample.csv -n 100
```

---

### `head`

Print the first N rows of a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats), or write them to a file by supplying an optional `OUTPUT` path.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Supported output formats (when writing to a file):** CSV (`.csv`), JSON (`.json`), YAML (`.yaml`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage (CLI):**

```sh
datu head <INPUT> [OUTPUT] [OPTIONS]
```

When `OUTPUT` is omitted, rows are printed to stdout and `--output` selects the display format. When `OUTPUT` is provided, rows are written to that file and the format is inferred from its extension (or `-O`/`--output-type`).

**Usage (REPL):**

```flt
read("file") |> head(n)
```

Prints the first _n_ rows to stdout. Chain `|> write("out.csv")` to write to a file, e.g. `read("data.parquet") |> head(10) |> write("first10.csv")`.

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Display format when printing to stdout: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. Cannot be combined with an `OUTPUT` file. |
| `-O`, `--output-type <TYPE>` | Output file type when writing to a file (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--json-pretty` | When writing JSON to a file, format output with indentation and newlines. Ignored for other output formats. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# First 10 rows (default)
datu head data.parquet

# First 100 rows
datu head data.parquet -n 100
datu head data.avro --number 100
datu head data.csv -n 100
datu head data.orc --number 100

# First 20 rows, specific columns
datu head data.parquet -n 20 --select id,name,email

# Head from a headerless CSV file
datu head data.csv --input-headers=false

# Write the first N rows to a file (format from extension)
datu head data.parquet first100.csv -n 100
datu head data.parquet first10.avro -n 10
```

---

### `tail`

Print the last N rows of a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats), or write them to a file by supplying an optional `OUTPUT` path.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Supported output formats (when writing to a file):** CSV (`.csv`), JSON (`.json`), YAML (`.yaml`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

> **Note:** For Avro and CSV files, `tail` requires a full file scan since these formats do not support random access to the end of the file.

**Usage (CLI):**

```sh
datu tail <INPUT> [OUTPUT] [OPTIONS]
```

When `OUTPUT` is omitted, rows are printed to stdout and `--output` selects the display format. When `OUTPUT` is provided, rows are written to that file and the format is inferred from its extension (or `-O`/`--output-type`).

**Usage (REPL):**

> `read("file") |> tail(n)`
>
> Prints the last _n_ rows to stdout. Chain `|> write("out.csv")` to write to a file, e.g. `read("data.parquet") |> tail(10) |> write("last10.csv")`.

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Display format when printing to stdout: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. Cannot be combined with an `OUTPUT` file. |
| `-O`, `--output-type <TYPE>` | Output file type when writing to a file (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `--json-pretty` | When writing JSON to a file, format output with indentation and newlines. Ignored for other output formats. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: `true`. Use `--sparse=false` to include default values. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |
| `--input-headers [BOOL]` | For CSV input: whether the first row is a header. Default: `true` when omitted. Use `--input-headers=false` for headerless CSV. |

**Examples:**

```sh
# Last 10 rows (default)
datu tail data.parquet

# Last 50 rows
datu tail data.parquet -n 50
datu tail data.avro --number 50
datu tail data.csv -n 50
datu tail data.orc --number 50

# Last 20 rows, specific columns
datu tail data.parquet -n 20 --select id,name,email

# Write the last N rows to a file (format from extension)
datu tail data.parquet last1000.csv -n 1000
datu tail data.parquet last10.parquet -n 10
```

---

### Version

Print the installed `datu` version.

**Usage (CLI):**

```sh
datu version
```

**Usage (REPL):**

> No equivalent; run `datu version` from the shell.

## Interactive Mode (REPL)

Running `datu` without any command starts an interactive REPL (Read-Eval-Print Loop):

```sh
datu
>
```

In the REPL, you compose data pipelines using the `|>` (pipe) operator to chain functions together. The general pattern is:

```text
read("input") |> ... |> write("output")
```

### Functions

#### `read(path)`

Read a data file. Supported formats: Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), JSON (`.json`), ORC (`.orc`). CSV files are assumed to have a header row by default.

```text
> read("data.parquet") |> write("data.csv")
> read("data.csv") |> write("data.parquet")
```

#### `write(path)`

Write data to a file. The output format is inferred from the file extension. Supported formats: CSV (`.csv`), JSON (`.json`), YAML (`.yaml`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

```text
> read("data.parquet") |> write("output.json")
```

#### `select(columns...)`

Select and reorder columns. Columns can be specified using symbol syntax (`:name`) or string syntax (`"name"`).

```text
> read("data.parquet") |> select(:id, :email) |> write("subset.csv")
> read("data.parquet") |> select("id", "email") |> write("subset.csv")
```

Columns appear in the output in the order they are listed, so `select` can also be used to reorder columns:

```text
> read("data.parquet") |> select(:email, :id) |> write("reordered.csv")
```

With optional `group_by(...)`, you can use aggregates in `select`: `sum`, `avg`, `min`, `max`, `count` (non-null values in a column), and `count_distinct` (distinct non-null values). A `select` of only aggregates (no `group_by`) summarizes the whole table.

```text
> read("data.parquet") |> select(count(:id))
> read("data.parquet") |> group_by(:country) |> select(:country, count_distinct(:user_id))
```

#### `head(n)`

Take the first _n_ rows.

```text
> read("data.parquet") |> head(10) |> write("first10.csv")
```

#### `sample(n)`

Take _n_ random rows from the data. Default: 10.

```text
> read("data.parquet") |> sample(5) |> write("sampled.csv")
```

#### `tail(n)`

Take the last _n_ rows.

```text
> read("data.parquet") |> tail(10) |> write("last10.csv")
```

### Composing Pipelines

Functions can be chained in any order to build more complex pipelines:

```text
> read("users.avro") |> select(:id, :first_name, :email) |> head(5) |> write("top5.json")
> read("data.parquet") |> select(:two, :one) |> tail(1) |> write("last_row.csv")
> read("data.parquet") |> select(:id, :email) |> sample(5) |> write("sample.csv")
```

---

## How it Works Internally

Internally, `datu` constructs a pipeline based on the command and arguments. When possible, it uses the [Datafusion DataFrame API](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html) for efficiency and performance. However, ORC, .XLSX, YAML, and JSON (pretty) aren't natively supported by Datafusion, and `datu` uses internal adapters for those file formats.


For example, the following invocation

```sh
datu convert input.parquet output.yaml --select id,name,email
```

constructs a pipeline that's composed of:
  - a (DataFrame) Parquet reader step that reads the `input.parquet` file and filters for only the `id`, `name`, and `email` columns, that chains to
  - a YAML writer step, that writes the `id`, `name`, and `email` columns from `input.parquet` to `output.csv`
