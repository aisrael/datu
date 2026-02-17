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

## How it Works Internally

Internally, `datu` constructs a pipeline based on the command and arguments.


For example, the following invocation

```sh
datu convert input.parquet output.csv --select id,name,email
```

constructs a pipeline that composed of:
  - a parquet reader step that reads the `input.parquet` file then chains to
  - a "select column" step that filters for only the `id`, `name`, and `email` columns, then finally
  - a CSV writer step, that writes the `id`, `name`, and `email` columns from `input.parquet` to `output.csv`

## Supported Formats

| Format                        | Read | Write | Display |
|-------------------------------|:----:|:-----:|:-------:|
| Parquet (`.parquet`, `.parq`) |  ✓   |   ✓   |    —    |
| Avro (`.avro`)                |  ✓   |   ✓   |    —    |
| ORC (`.orc`)                  |  ✓   |   ✓   |    —    |
| XLSX (`.xlsx`)                |  —   |   ✓   |    —    |
| CSV (`.csv`)                  |  —   |   ✓   |    ✓    |
| JSON (`.json`)                |  —   |   ✓   |    ✓    |
| JSON (pretty)                 |  —   |   —   |    ✓    |
| YAML                          |  —   |   —   |    ✓    |

- **Read** — Input file formats for `convert`, `count`, `schema`, `head`, and `tail`.
- **Write** — Output file formats for `convert`.
- **Display** — Output format when printing to stdout (`schema`, `head`, `tail` via `--output`: csv, json, json-pretty, yaml).

## Examples

### `schema`

Display the schema of a Parquet, Avro, or ORC file (column names, types, and nullability). Useful for inspecting file structure without reading data.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`).

**Usage:**

```sh
datu schema <FILE> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |

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

Return the number of rows in a Parquet, Avro, or ORC file.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`).

**Usage:**

```sh
datu count <FILE>
```

**Examples:**

```sh
# Count rows in a Parquet file
datu count data.parquet

# Count rows in an Avro or ORC file
datu count events.avro
datu count data.orc
```

---

### `convert`

Convert data between supported formats. Input and output formats are inferred from file extensions.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`).

**Supported output formats:** CSV (`.csv`), JSON (`.json`), Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`), XLSX (`.xlsx`).

**Usage:**

```sh
datu convert <INPUT> <OUTPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are written. Column names can be given as multiple arguments or as comma-separated values (e.g. `--select id,name,email` or `--select id --select name --select email`). |
| `--limit <N>` | Maximum number of records to read from the input. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: true. Use `--sparse=false` to include default values (e.g. empty string). |
| `--json-pretty` | When converting to JSON, format output with indentation and newlines. Ignored for other output formats. |

**Examples:**

```sh
# Parquet to CSV (all columns)
datu convert data.parquet data.csv

# Parquet to Avro (first 1000 rows)
datu convert data.parquet data.avro --limit 1000

# Avro to CSV, only specific columns
datu convert events.avro events.csv --select id,timestamp,user_id

# Parquet to Parquet with column subset
datu convert input.parq output.parquet --select one,two,three

# Parquet, Avro, or ORC to Excel (.xlsx)
datu convert data.parquet report.xlsx

# Parquet or Avro to ORC
datu convert data.parquet data.orc

# Parquet or Avro to JSON
datu convert data.parquet data.json
```

---

### `head`

Print the first N rows of a Parquet, Avro, or ORC file to stdout (default CSV; use `--output` for other formats).

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`).

**Usage:**

```sh
datu head <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: true. Use `--sparse=false` to include default values. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# First 10 rows (default)
datu head data.parquet

# First 100 rows
datu head data.parquet -n 100
datu head data.avro --number 100
datu head data.orc --number 100

# First 20 rows, specific columns
datu head data.parquet -n 20 --select id,name,email
```

---

### `tail`

Print the last N rows of a Parquet, Avro, or ORC file to stdout (default CSV; use `--output` for other formats).

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`), ORC (`.orc`).

> **Note:** For Avro files, `tail` requires a full file scan since Avro does not support random access to the end of the file.

**Usage:**

```sh
datu tail <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--output <FORMAT>` | Output format: `csv`, `json`, `json-pretty`, or `yaml`. Case insensitive. Default: `csv`. |
| `--sparse` | For JSON/YAML: omit keys with null/missing values. Default: true. Use `--sparse=false` to include default values. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# Last 10 rows (default)
datu tail data.parquet

# Last 50 rows
datu tail data.parquet -n 50
datu tail data.avro --number 50
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
