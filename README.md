dtfu - a data multi-tool
=======================

`dtfu` is intended to be a lightweight, fast, and versatile CLI tool for reading, querying, and converting data in various file formats, such as Parquet, .XLSX, CSV, and even f3.

It can be used interactively, with a REPL-like interface, or, non-interactively with all arguments given on the CLI or via an input script, for automated pipelines.

Internally, it also uses a pipeline architecture that aids in extensibility and testing, as well as allowing for parallel processing even of large datasets, if the input/output formats support it.

## How it Works Internally

`dtfu` has an internal, streaming, pipeline model (a DAG).

### Parsing and Constructing the Model

It can construct the model from the CLI arguments or from the REPL (using their respective `PipelineModeler` trait implementations).

The CLI is parsed using the popular [clap](https://docs.rs/clap) crate, while the REPL uses a "lite functional" DSL inspired by Elixir.

For example, the following CLI arguments

```sh
dtfu -i input.parq --select id,name,email -o output.csv
```

Are functionally equivalent to the REPL

```
> READ("input.parq") |> SELECT(:id, :name, :email) |> WRITE("output.csv")
```

In both cases, the modeler constructs the following simplified pipeline model:

```mermaid
flowchart LR
    R["READ(#quot;input.parq#quot;)"] --> S
    S["SELECT(:id, :name, :email)"] --> W["WRITE(#quot;output.csv#quot;)"]
```

### Model Execution

Once the model is executed, the pipeline is executed in one of two ways.

In "serial" mode (default), the various stages of the pipeline read, process, and write data sequentially, using a push-based approach (internally implemented using Rust channels). This usually provides the highest 'raw' performance with optimal resource consumption.

In "parallel" mode, for pipeline stages that support it, large datasets can be partitioned and processed in parallel. This can provide greater throughput, especially for larger datasets and when running on multiple cores, at the expense of slightly more resource consumption.

Which execution method to use depends on the data, the pipeline, and the desired outcomes. Indeed, it's common to chain multiple, sequential pipelines together (using external scripts or orchestration tools), first to partition the input data, then to process it in parallel across multiple machines, then aggregate and write the consolidated output.

### Retry

Data and models aren't perfect, and machines can sometimes hiccup. Most commonly, a `dtfu` process might run out of memory or disk space while processing a large dataset or writing its output due to some temporary resource exhaustion.

To mitigate this, `dtfu` supports a "retryable" mode.

In "retryable" mode, `dtfu` periodically writes its progress, including intermediate computation results, to disk.

In case of an error or disruption, `dtfu` can then be instructed to attempt to resume from where it left off. If not possible, `dtfu` will print an error message and abort.

Note that, on a system with high disk utilisation pressure, "retry" mode _will_ require _even more_ disk space to store intermediate state.

However, for extremely large datasets running on, say, ephemeral machines (VMs or containers) that can be interrupted at any time, "retry" mode can save having to rerun a long-running pipeline from the start, saving hours of lost work.
