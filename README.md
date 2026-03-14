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
| XLSX (`.xlsx`)                |  —   |   ✓   |    —    |
| JSON (`.json`)                |  —   |   ✓   |    ✓    |
| JSON (pretty)                 |  —   |   —   |    ✓    |
| YAML                          |  —   |   —   |    ✓    |

- **Read** — Input file formats for `convert`, `count`, `schema`, `head`, and `tail`.
- **Write** — Output file formats for `convert`.
- **Display** — Output format when printing to stdout (`schema`, `head`, `tail` via `--output`: csv, json, json-pretty, yaml).

**File type detection:** By default, file types are inferred from extensions. Use `--input <TYPE>` (`-I`) to override input format detection, and `--output <TYPE>` (`-O`, `convert` only) to override output format detection. Valid types: `avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`.

**CSV options:** When reading CSV files, the `--input-headers` option controls whether the first row is treated as column names. Omitted or `--input-headers` means true (header present); `--input-headers=false` for headerless CSV. Applies to `convert`, `count`, `schema`, `head`, and `tail`.

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

### `schema`

Display the schema of a Parquet, Avro, CSV, or ORC file (column names, types, and nullability). Useful for inspecting file structure without reading data. CSV schema uses type inference from the data.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Usage:**

```sh
datu schema <FILE> [OPTIONS]
```

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

**Usage:**

```sh
datu count <FILE> [OPTIONS]
```

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

### `convert`

Convert data between supported formats. Input and output formats are inferred from file extensions, or can be specified explicitly with `--input` and `--output`.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage:**

```sh
datu convert <INPUT> <OUTPUT> [OPTIONS]
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

# Parquet, Avro, CSV, or ORC to Excel (.xlsx)
datu convert data.parquet report.xlsx

# Parquet or Avro to ORC
datu convert data.parquet data.orc

# Parquet or Avro to JSON
datu convert data.parquet data.json
```

---

### `sample`

Print N randomly sampled rows from a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats). For Parquet and ORC, sampling uses file metadata to determine total row count and selects random indices; for Avro and CSV, reservoir sampling is used.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Usage:**

```sh
datu sample <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to sample. Default: 10. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
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
```

---

### `head`

Print the first N rows of a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats).

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

**Usage:**

```sh
datu head <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
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
```

---

### `tail`

Print the last N rows of a Parquet, Avro, CSV, or ORC file to stdout (default CSV; use `--output` for other formats).

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`).

> **Note:** For Avro and CSV files, `tail` requires a full file scan since these formats do not support random access to the end of the file.

**Usage:**

```sh
datu tail <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-I`, `--input <TYPE>` | Input file type (`avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`). Overrides extension-based detection. |
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
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

# Redirect tail output to a file
datu tail data.parquet -n 1000 > last1000.csv
```

---

### Version

Print the installed `datu` version:

```sh
datu version
```

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

Read a data file. Supported formats: Parquet (`.parquet`, `.parq`), Avro (`.avro`), CSV (`.csv`), ORC (`.orc`). CSV files are assumed to have a header row by default.

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

Internally, `datu` constructs a pipeline based on the command and arguments.


For example, the following invocation

```sh
datu convert input.parquet output.csv --select id,name,email
```

constructs a pipeline that's composed of:
  - a parquet reader step that reads the `input.parquet` file then chains to
  - a "select column" step that filters for only the `id`, `name`, and `email` columns, then finally
  - a CSV writer step, that writes the `id`, `name`, and `email` columns from `input.parquet` to `output.csv`
