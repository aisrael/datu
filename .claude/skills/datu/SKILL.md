---
name: datu
description: Inspect, validate, convert, compare, sample, concatenate, and split data files (Parquet, Avro, ORC, CSV, JSON, XLSX) from the shell using the `datu` CLI. Use whenever a task involves looking inside a binary data file (schema, row count, first/last/random rows), converting between formats or compression codecs, checking that a conversion or pipeline output is correct, diffing two datasets, or merging/partitioning files. Prefer this over writing ad-hoc Python/pandas/pyarrow scripts for these jobs.
---

# datu — data file CLI for agents

`datu` is a fast, single-binary CLI for reading, inspecting, and converting columnar and tabular data files. Use it instead of writing throwaway pandas/pyarrow/avro-tools scripts. This skill covers the non-interactive CLI only; never launch bare `datu` with no subcommand, because that starts an interactive REPL and blocks.

## Setup check

```sh
datu --version          # confirm it is installed
cargo install datu      # if missing (requires Rust)
```

Run `datu <command> --help` if a flag below is rejected. Older installs may lack `concat`, `split`, and the `--output-*-compression` flags.

## Formats and type detection

| Format  | Extensions          | Read | Write |
|---------|---------------------|:----:|:-----:|
| Parquet | `.parquet`, `.parq` |  ✓   |   ✓   |
| Avro    | `.avro`             |  ✓   |   ✓   |
| ORC     | `.orc`              |  ✓   |   ✓   |
| CSV     | `.csv`              |  ✓   |   ✓   |
| JSON    | `.json`             |  ✓   |   ✓   |
| XLSX    | `.xlsx`             |  —   |   ✓   |
| YAML    | `.yaml`             |  —   | ✓ (head/tail/sample only) |

- The format is inferred from the file extension. For files without a standard extension (for example `part-00000`, `data.snappy`, or temp files), pass `-I/--input <type>`. For output, use `-O` (`--output` on convert/concat/split, `--output-type` on head/tail/sample).
- Valid types: `avro`, `csv`, `json`, `orc`, `parquet`, `xlsx`, `yaml`.
- For headerless CSV, add `--input-headers=false` to any reading command.
- JSON input means a JSON array of objects.

## Commands at a glance

| Goal | Command |
|------|---------|
| Column names, types, and nullability | `datu schema FILE [-o json\|json-pretty\|yaml]` |
| Row count | `datu count FILE` |
| First / last N rows | `datu head FILE -n N`, `datu tail FILE -n N` |
| N random rows | `datu sample FILE -n N` |
| Convert format, subset columns, or recompress | `datu convert IN OUT [--select a,b] [--limit N]` |
| Compare two datasets | `datu diff A B --json` |
| Merge many files into one | `datu concat IN... OUT` (globs allowed) |
| Partition one file into many | `datu split IN [OUT] --split N\|64mb` |

## Inspection (read-only, safe to run freely)

```sh
datu schema data.parquet                 # "name: TYPE, nullable" per line
datu schema data.avro -o json            # machine-readable: [{name, data_type, nullable, ...}]
datu count data.orc                      # prints a single integer
datu head data.parquet -n 5 -o json      # rows as a JSON array of objects
datu tail events.avro -n 20 --select id,ts
datu sample big.parquet -n 50 -o json    # random rows, good for spot checks
```

Tips for agents:
- Prefer `-o json` (compact, one line) when you will parse the output, and `-o json-pretty` or `yaml` when you only need to read it.
- **JSON output has no trailing newline.** Add `; echo` if you print anything after it.
- **Null handling:** `--sparse` defaults to `true`, so JSON/YAML output *omits* keys whose value is null. A missing key means null, not a missing column. Pass `--sparse=false` to emit explicit `null`s, which you want when checking for nulls or comparing row shapes.
- Keep output small: always use `-n` and `--select` on wide or large files instead of dumping everything.
- `count` and `head` are cheap on Parquet/ORC (metadata or random access). `tail` on Avro/CSV scans the whole file.
- `schema` on CSV/JSON reports *inferred* types, and JSON columns may come back in a different order than in the file.

## Conversion

```sh
datu convert data.parquet data.csv
datu convert data.csv data.parquet                       # CSV types are inferred
datu convert events.avro events.parquet --output-parquet-compression zstd
datu convert data.parquet data.avro --output-avro-compression snappy
datu convert data.parquet subset.parquet --select id,email --limit 1000
datu convert data.parquet report.xlsx
datu convert data.parquet data.json --json-pretty
datu convert raw_blob out.csv -I parquet                 # no extension on input
```

- `--select` accepts `a,b,c` or repeated `--select a --select b`. The output column order follows the selection.
- Compression codecs: Parquet `none|snappy|gzip|zstd|brotli|lz4|lz4_raw`; Avro `none|deflate|snappy`. The default is `none` for both. These flags are also accepted by `concat` and `split`.
- `convert` overwrites `OUT` without asking. Check the path before running it.
- `head`, `tail`, and `sample` can also write files: `datu head big.parquet first100.avro -n 100`. The `-o` stdout format option cannot be combined with an output path.

## Validation workflows

**Verify a conversion or transformation preserved the data:**

```sh
datu count in.avro; datu count out.parquet               # row counts match?
datu schema in.avro -o json; datu schema out.parquet -o json   # compare types
datu diff in.avro roundtrip.avro --json --limit 0        # exact row-level check
```

To round-trip check a format change, convert back to the original format and then `diff` the two same-format files.

**Diff semantics (important):**
- `datu diff` **exits 0 whether or not the files differ.** It exits non-zero only on errors. Always use `--json` and read the `identical` field:
  ```sh
  datu diff a.parquet b.parquet --json | jq -e '.identical' >/dev/null && echo SAME || echo DIFFERENT
  ```
- The comparison is row-set based (unordered), over **common columns only**. Columns that exist in only one file appear under `schema.only_in_file1/only_in_file2` and do not by themselves make the rows differ.
- JSON result fields: `identical`, `row_count` (when identical), `columns`, `schema`, `only_in_file1`, `only_in_file2`, `truncated`, `limit`. Values in the row objects are rendered as strings.
- The default `--limit 100` stops early and **may report false positives**. Use `--limit 0` for an exact answer, but it holds every distinct row in memory, so on huge files compare `count` and `schema` first, or `diff` a `--limit`ed `convert` of each side.
- Both inputs must be the same format (a single `-I` applies to both). To compare across formats, `convert` one side first.

**Quick sanity checks:**
- Nulls in a column: `datu head f.parquet -n 1000 --select col -o json --sparse=false | jq '[.[] | select(.col == null)] | length'`
- Distinct values or other aggregates: pipe `-o json` into `jq`. datu has no filter or aggregate commands.
- Header problems in CSV: if `schema` shows columns named like data values, re-run with `--input-headers=false`. If it shows `column_1...`, the file probably has a header you disabled.

## Concat and split

```sh
datu concat part-*.avro all.parquet                      # last arg is the output
datu concat "2024-*.csv" late.csv all-2024.csv           # quote globs to let datu expand them
datu split big.avro --split 100000                       # -> big.part00001.avro, big.part00002.avro, ...
datu split big.parquet out/chunk.parquet --split 64mb    # size-based; kb/mb/gb or kib/mib/gib
datu split big.avro out/part.parquet --split 50000 --output-parquet-compression zstd
```

- `concat` inputs must have union-compatible schemas. Check with `schema` first if unsure.
- `split` names partitions `<base>.partNNNNN.<ext>`, 1-based, next to the input by default. It does **not** create the output directory, so run `mkdir -p out/` first. Otherwise it fails with "No such file or directory". Byte-size splits are approximate because they are estimated from in-memory size, not on-disk size.
- Verify afterwards: the sum of `datu count` over the parts should equal the count of the original.

## Errors

A failure prints `Error: ...` with a source location to stderr and exits with status 1. Common causes: a wrong path, an unknown extension (fix with `-I`/`-O`), an unsupported direction (for example, reading XLSX), or incompatible schemas in `concat`.
