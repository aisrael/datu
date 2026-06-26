//! `datu diff` — compare two data files row-by-row

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use clap::Args;
use datu::FileType;
use datu::pipeline::Producer;
use datu::pipeline::build_reader;
use datu::pipeline::read::ReadArgs;
use datu::pipeline::read::ReadResult;
use datu::pipeline::read::read_to_dataframe;
use datu::resolve_file_type;
use eyre::bail;

/// Arguments for the `datu diff` command.
#[derive(Args)]
pub struct DiffArgs {
    /// Path to the first file
    pub file1: String,
    /// Path to the second file
    pub file2: String,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type override applied to both files."
    )]
    pub input: Option<FileType>,
}

/// Reads a file into a `Vec<RecordBatch>`, dispatching between the record-batch
/// path (Parquet, Avro, CSV, ORC) and the DataFusion path (JSON).
async fn read_batches(path: &str, file_type: FileType) -> eyre::Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Json => {
            let result = read_to_dataframe(path, FileType::Json, None).await?;
            let ReadResult::DataFrame(mut source) = result else {
                bail!("expected DataFrame for JSON file {path}");
            };
            let df = source.get().await?;
            Ok(df.collect().await?)
        }
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc => {
            let read_args = ReadArgs::new(path, file_type);
            let mut reader = build_reader(&read_args)?;
            let batch_reader = reader.get().await?;
            let mut batches = Vec::new();
            for batch in batch_reader {
                batches.push(batch?);
            }
            Ok(batches)
        }
        _ => bail!("unsupported file type for diff: {file_type}"),
    }
}

/// Formats a single row from a `RecordBatch` as a tab-separated string.
fn row_to_string(batch: &RecordBatch, row: usize) -> String {
    let format_options = FormatOptions::default();
    (0..batch.num_columns())
        .map(|col| {
            let column = batch.column(col);
            let formatter = ArrayFormatter::try_new(column.as_ref(), &format_options)
                .expect("ArrayFormatter creation should not fail");
            formatter.value(row).to_string()
        })
        .collect::<Vec<_>>()
        .join("\t")
}

/// The `datu diff` command: compares two data files and reports row-level differences.
pub async fn diff(args: DiffArgs) -> eyre::Result<()> {
    // Step 1: resolve and validate format
    let ft1 = resolve_file_type(args.input, &args.file1)?;
    let ft2 = resolve_file_type(args.input, &args.file2)?;

    if ft1 != ft2 {
        eprintln!(
            "Error: files have different formats: {} is {ft1}, {} is {ft2}",
            args.file1, args.file2
        );
        bail!(
            "files have different formats: {} is {ft1}, {} is {ft2}",
            args.file1,
            args.file2
        );
    }

    // Step 2: read both files
    let batches1 = read_batches(&args.file1, ft1).await?;
    let batches2 = read_batches(&args.file2, ft2).await?;

    // Step 3: compare schemas
    let schema1 = batches1
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| arrow::datatypes::Schema::empty().into());
    let schema2 = batches2
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| arrow::datatypes::Schema::empty().into());

    let cols1: Vec<(String, arrow::datatypes::DataType)> = schema1
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let cols2: Vec<(String, arrow::datatypes::DataType)> = schema2
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();

    let set1: std::collections::HashSet<_> = cols1.iter().cloned().collect();
    let set2: std::collections::HashSet<_> = cols2.iter().cloned().collect();

    let only_in_1: Vec<_> = cols1.iter().filter(|c| !set2.contains(c)).collect();
    let only_in_2: Vec<_> = cols2.iter().filter(|c| !set1.contains(c)).collect();
    let common: Vec<_> = cols1.iter().filter(|c| set2.contains(c)).collect();

    if !only_in_1.is_empty() || !only_in_2.is_empty() {
        eprintln!("Schema differences:");
        if !only_in_1.is_empty() {
            eprintln!("  Only in {}:", args.file1);
            for (name, dt) in &only_in_1 {
                eprintln!("    {name}: {dt}");
            }
        }
        if !only_in_2.is_empty() {
            eprintln!("  Only in {}:", args.file2);
            for (name, dt) in &only_in_2 {
                eprintln!("    {name}: {dt}");
            }
        }
    }

    if common.is_empty() {
        eprintln!(
            "Error: No common columns between {} and {}",
            args.file1, args.file2
        );
        bail!(
            "No common columns between {} and {}",
            args.file1,
            args.file2
        );
    }

    // Step 4: project to common columns if schemas differ
    let common_names: Vec<&str> = common.iter().map(|(name, _)| name.as_str()).collect();

    let batches1 = if !only_in_1.is_empty() || !only_in_2.is_empty() {
        project_batches(&batches1, &schema1, &common_names)?
    } else {
        batches1
    };
    let batches2 = if !only_in_1.is_empty() || !only_in_2.is_empty() {
        project_batches(&batches2, &schema2, &common_names)?
    } else {
        batches2
    };

    // Step 5: compare rows
    let mut counts: HashMap<String, (i64, i64)> = HashMap::new();
    let mut total1: usize = 0;

    for batch in &batches1 {
        for row in 0..batch.num_rows() {
            let key = row_to_string(batch, row);
            counts.entry(key).or_insert((0, 0)).0 += 1;
            total1 += 1;
        }
    }

    for batch in &batches2 {
        for row in 0..batch.num_rows() {
            let key = row_to_string(batch, row);
            counts.entry(key).or_insert((0, 0)).1 += 1;
        }
    }

    let all_equal = counts.values().all(|(c1, c2)| c1 == c2);

    if all_equal {
        let row_label = if total1 == 1 { "row" } else { "rows" };
        println!("Files are identical ({total1} {row_label})");
    } else {
        let header = common_names.join("\t");

        let mut only_file1: Vec<&String> = Vec::new();
        let mut only_file2: Vec<&String> = Vec::new();

        for (row_str, (c1, c2)) in &counts {
            if c1 > c2 {
                for _ in 0..(c1 - c2) {
                    only_file1.push(row_str);
                }
            } else if c2 > c1 {
                for _ in 0..(c2 - c1) {
                    only_file2.push(row_str);
                }
            }
        }

        only_file1.sort();
        only_file2.sort();

        if !only_file1.is_empty() {
            let count = only_file1.len();
            let row_label = if count == 1 { "row" } else { "rows" };
            println!("Only in {} ({count} {row_label}):", args.file1);
            println!("  {header}");
            for row_str in &only_file1 {
                println!("  {row_str}");
            }
        }

        if !only_file1.is_empty() && !only_file2.is_empty() {
            println!();
        }

        if !only_file2.is_empty() {
            let count = only_file2.len();
            let row_label = if count == 1 { "row" } else { "rows" };
            println!("Only in {} ({count} {row_label}):", args.file2);
            println!("  {header}");
            for row_str in &only_file2 {
                println!("  {row_str}");
            }
        }
    }

    Ok(())
}

/// Projects each batch down to only the named columns, preserving the given order.
fn project_batches(
    batches: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    column_names: &[&str],
) -> eyre::Result<Vec<RecordBatch>> {
    let indices: Vec<usize> = column_names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|e| eyre::eyre!("column {name} not found: {e}"))
        })
        .collect::<eyre::Result<Vec<_>>>()?;

    batches
        .iter()
        .map(|batch| batch.project(&indices).map_err(Into::into))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_identical_file() {
        let args = DiffArgs {
            file1: "fixtures/table.parquet".to_string(),
            file2: "fixtures/table.parquet".to_string(),
            input: None,
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_identical_csv() {
        let args = DiffArgs {
            file1: "fixtures/table.csv".to_string(),
            file2: "fixtures/table.csv".to_string(),
            input: None,
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_format_mismatch() {
        let args = DiffArgs {
            file1: "fixtures/table.csv".to_string(),
            file2: "fixtures/table.parquet".to_string(),
            input: None,
        };
        let result = diff(args).await;
        assert!(result.is_err(), "diff should fail for different formats");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("different formats"),
            "error should mention different formats, got: {err_msg}"
        );
    }
}
