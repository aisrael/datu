dtfu - a data file utility
=======================

`dtfu` is intended to be a lightweight, fast, and versatile CLI tool for reading, querying, and converting data in various file formats, such as Parquet, .XLSX, CSV, and even f3.

It is used non-interactively: you invoke a subcommand with arguments on the CLI or from scripts for automated pipelines.

Internally, it also uses a pipeline architecture that aids in extensibility and testing, as well as allowing for parallel processing even of large datasets, if the input/output formats support it.

## How it Works Internally

Internally, `dtfu` constructs a pipeline based on the command and arguments.


For example, the following invocation

```sh
dtfu convert input.parquet output.csv --select id,name,email
```

constructs a pipeline that reads the input, selects only the specified columns, and writes the output.

## Examples

### `schema`

Display the schema of a Parquet or Avro file (column names, types, and nullability). Useful for inspecting file structure without reading data.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
dtfu schema <FILE> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--output <FORMAT>` | Output format: `csv`, `json`, or `yaml`. Case insensitive. Default: `csv`. |

**Output formats:**

- **csv** (default): One line per column, e.g. `name: String (UTF8), nullable`.
- **json**: Pretty-printed JSON array of objects with `name`, `data_type`, `nullable`, and optionally `converted_type` (Parquet).
- **yaml**: YAML list of mappings with the same fields.

**Examples:**

```sh
# Default CSV-style output
dtfu schema data.parquet

# JSON output
dtfu schema data.parquet --output json

# YAML output (e.g. for config or tooling)
dtfu schema events.avro --output yaml
dtfu schema events.avro -o YAML
```

---

### `convert`

Convert data between supported formats. Input and output formats are inferred from file extensions.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Supported output formats:** CSV (`.csv`), Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
dtfu convert <INPUT> <OUTPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are written. Column names can be given as multiple arguments or as comma-separated values (e.g. `--select id,name,email` or `--select id --select name --select email`). |
| `--limit <N>` | Maximum number of records to read from the input. |

**Examples:**

```sh
# Parquet to CSV (all columns)
dtfu convert data.parquet data.csv

# Parquet to Avro (first 1000 rows)
dtfu convert data.parquet data.avro --limit 1000

# Avro to CSV, only specific columns
dtfu convert events.avro events.csv --select id,timestamp,user_id

# Parquet to Parquet with column subset
dtfu convert input.parq output.parquet --select one,two,three
```

---

### `head`

Print the first N rows of a Parquet or Avro file as CSV to stdout.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
dtfu head <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# First 10 rows (default)
dtfu head data.parquet

# First 100 rows
dtfu head data.parquet -n 100
dtfu head data.avro --number 100

# First 20 rows, specific columns
dtfu head data.parquet -n 20 --select id,name,email
```

---

### `tail`

Print the last N rows of a Parquet or Avro file as CSV to stdout.

**Supported input formats:** Parquet (`.parquet`, `.parq`), Avro (`.avro`).

**Usage:**

```sh
dtfu tail <INPUT> [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-n`, `--number <N>` | Number of rows to print. Default: 10. |
| `--select <COLUMNS>...` | Columns to include. If not specified, all columns are printed. Same format as `convert --select`. |

**Examples:**

```sh
# Last 10 rows (default)
dtfu tail data.parquet

# Last 50 rows
dtfu tail data.parquet -n 50
dtfu tail data.avro --number 50

# Last 20 rows, specific columns
dtfu tail data.parquet -n 20 --select id,name,email

# Redirect tail output to a file
dtfu tail data.parquet -n 1000 > last1000.csv
```

---

### Version

Print the installed `dtfu` version:

```sh
dtfu version
```
