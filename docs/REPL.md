datu REPL
====

When run without any CLI commands, `datu` starts up in REPL mode. `datu`'s REPL is built around an experimental 'lite' functional language, [flt](https://github.com/aisrael/flt), which is heavily inspired by Elixir, Ruby, and Rust.

## flt Basics

flt has basic types:
- Boolean: `true`, `false`
- Numbers: `0`, `-1`, `3.14`
- Strings: `""` (empty string), `"foo"`, `"escaped: \"bar\""`
- Symbols, which can be
  - plain symbols, `:foo`, `:bar`
  - quoted symbols, `:"_one"`, `:"has space"`
- Arrays—an ordered, integer-indexed collection of values:
  - `[0, "foo", :bar]`
  - to access values: `array[1] # -> "foo"`
- Maps—a collection that maps keys (typically, symbols) to values
  - `{id: 1, country_code: "CA"}`
  - to fetch the value for a key: `map[:id] # -> 1`

flt has typical operators:
- Unary: `-` (negation), `!` (boolean NOT)
- Binary: `+`, `-`, `*`, `/`
- Grouping (parentheses): `(`, `)`
- Array/Map indexing (square brackets): `[`, `]`
- Pipe operator: `|>` which is used to compose chains of functions

### Functions

A function call is expressed as the name of the function and its arguments:

```flt
head("input.parquet")
```

#### Function Arguments

Function arguments can be passed by position _or_ by name, similar to Python.

That is, given a hypothetical definition of `read()`:

```flt
fn read(path: String, type: ?FileType) # Here, the `?Filetype` denotes that `type` parameter is optional
```

We can call `read()` multiple ways:

```flt
read("input.parquet") # `type` is set to `None`
```

Function calls can also accept optional keyword arguments at the end:

```flt
head("input.parquet", n: 50)
```

The output of a function can be passed as the input to the next function using the pipe (`|>`) operator:

```flt
read("input.parquet") |> head(50)
```

#### Variadic Functions

Variadic functions are defined using a special syntax inspired by Ruby's 'splat' operator:

```flt
select(data: Data, *args)
```

#### Function Overloading

You might've noticed that the `head()` function can be called many ways. That's because flt supports function overloading—that is, declaring the same function with different signatures.

The `head()` function, though internally implemented in `datu`, can be thought of as having the following definitions:

```flt
fn head(path: String, n: int = 10) -> Data

fn head(previous: Data, n: int) -> Data

fn head(previous: Data, rest: KeywordList)
```

Here, a `Data` is a placeholder for the result of a previous step in the pipeline. The `read()` function can be thought of as having the following definition:

```flt
read(path: String) -> Data
```

The output of `read()` is a `Data`, which is the first argument to the second definition of `head()`, which is what allows them to be chained.

We can think of

```flt
read("input.parquet") |> head(10)
```

as desugaring to the equivalent

```flt
let data = read("input.parquet")
head(data, 10)
```

## datu Functions

For the following functions, note that the function signatures and types provided are for illustration purposes only. All functions in `datu` are internally implemented in Rust, and the actual types aren't very helpful for the purpose of documenting the REPL.

### Pipeline shape

A REPL pipeline must start with `read(...)`. You may use at most one `group_by(...)` and one `select(...)` per pipeline (in either order). Further stages—`head`, `tail`, `sample`, `schema`, `count`, or `write`—are added when needed (for example, `read("x.parquet") |> head(5)` skips `select` entirely). You cannot repeat `select` or `group_by` in the same pipeline. If `group_by(...)` appears, a matching `select(...)` is required.

### `read`

```flt
read(path: String, file_type: ?FileType) -> Data
```

Reads a Parquet, Avro, ORC, CSV, or JSON file at the given `path`. If `file_type` is not specified, will attempt to discern the input file type from the file extension.

#### Supported Input File Types and Extensions

| Extension             | File Type |
|-----------------------|-----------|
| `.parquet` or `.parq` | Parquet   |
| `.avro`               | Avro      |
| `.orc`                | ORC       |
| `.csv`                | CSV       |
| `.json`               | JSON      |

### `write`

```flt
write(data: Data, path: String, file_type: ?FileType) -> WriteResult
```

Writes data to a Parquet, Avro, ORC, CSV, JSON, or XLSX file at the given `path`. If `file_type` is not specified, will attempt to discern the output file type from the file extension.

#### Supported Output File Types and Extensions

| Extension             | File Type |
|-----------------------|-----------|
| `.parquet` or `.parq` | Parquet   |
| `.avro`               | Avro      |
| `.orc`                | ORC       |
| `.csv`                | CSV       |
| `.json`               | JSON      |
| `.xlsx`               | XLSX      |

### `select`

```flt
select(data: Data, *args)
```

Here, `*args` means 'collect the rest of the function arguments and provide them as the parameter `args` (with type `Array`)

`select()` can be used to specify which columns to select from the input data:

```flt
read("input.parquet") |> select(:id, :name, :email)
```

If the column name is specified as a `Symbol` (`:name`) or a bare identifier (`name`), then `select()` treats that name as case-insensitive and will match a column named `"NAME"`, `"name"`, or `"Name"`.

If the column name is specified as a `String` (`"_one"`), then `select()` performs an exact, case-sensitive match.

The same rules apply to column arguments inside `sum(...)`, `avg(...)`, `min(...)`, and `max(...)`.

#### Global aggregates (`sum` / `avg` / `min` / `max` without `group_by`)

With no `group_by()`, you may use `select()` with only aggregate arguments (`sum(...)`, `avg(...)`, `min(...)`, and/or `max(...)`). That aggregates the whole table (for example, one row of totals/averages/minima/maxima).

```flt
read("input.parquet") |> select(sum(:quantity))
read("input.parquet") |> select(avg(:amount))
read("input.parquet") |> select(min(:amount), max(:amount))
```

Mixing plain column projections with aggregates in the same `select()` without `group_by()` is not allowed—use `group_by()` for the key columns first, or use only aggregates for a global summary.

#### Grouped aggregates (`group_by` + `select`)

`group_by(:key1, ...)` is a separate pipeline step. Every column in `group_by` _MUST_ be included as a plain column in `select()`. Any other selected column must use an aggregate (`sum()`, `avg()`, `min()`, or `max()`).

```flt
read("input.parquet") |> group_by(:country_code) |> select(:country_code, avg(:amount))
```

`select(:country_code, avg(:amount)) |> group_by(:country_code)` is equivalent to the form above (and you can use `sum()`, `min()`, or `max()` instead of `avg()` where appropriate).

If `group_by()` is present but `select()` lists only key columns (no aggregates), the statement is still valid (distinct group keys); the REPL prints this warning to stderr:

`warning: group_by() with no aggregates in select(); showing distinct group keys only (behavior may change)`

### Data preview (`head`, `tail`, and `sample`)

`head`, `tail`, and `sample` can either be used after a `read() |> ` expression, or, by themselves by providing the path as the first argument.

These take a row count and, when used without a following `write()`, print a slice of the pipeline result to stdout (an implicit print step is appended).

Only one of `head`, `tail`, or `sample` may appear in a pipeline.

| function   | description                            |
|------------|----------------------------------------|
| `head()`   | First _n_ rows                         |
| `tail()`   | Last _n_ rows                          |
| `sample()` | Takes a random sample of _n_ rows      |

```flt
read("input.parquet") |> head()  # default 10

read("input.parquet") |> head(5) # first 5 rows

head("input.parquet", 5)         # equivalent to the previous statement
```

### Metadata functions: `schema` and `count`

`schema` prints the table schema.

`count` prints the row count. Neither takes arguments.
