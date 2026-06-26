//! `datu diff` — compare two data files row-by-row

use std::collections::HashMap;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use clap::Args;
use datu::FileType;
use datu::pipeline::Producer;
use datu::pipeline::VecRecordBatchReader;
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
    #[arg(
        long,
        default_value_t = 100,
        help = "Maximum total differing rows to display (file1 + file2 combined). Use 0 for unlimited."
    )]
    pub max_diffs: usize,
}

/// Opens a lazy `RecordBatchReader` without loading all rows into memory.
///
/// JSON is the exception: DataFusion collects all batches internally, so we wrap
/// the resulting `Vec` in a `VecRecordBatchReader` to give a uniform iterator interface.
async fn open_reader(
    path: &str,
    file_type: FileType,
) -> eyre::Result<Box<dyn RecordBatchReader + 'static>> {
    match file_type {
        FileType::Json => {
            let result = read_to_dataframe(path, FileType::Json, None).await?;
            let ReadResult::DataFrame(mut source) = result else {
                bail!("expected DataFrame for JSON file {path}");
            };
            let df = source.get().await?;
            let batches = df.collect().await?;
            Ok(Box::new(VecRecordBatchReader::new(batches)))
        }
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc => {
            let read_args = ReadArgs::new(path, file_type);
            let mut reader_source = build_reader(&read_args)?;
            Ok(reader_source.get().await?)
        }
        _ => bail!("unsupported file type for diff: {file_type}"),
    }
}

/// Formats a single row as a tab-separated string for use as a map key.
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

/// Returns the column positions in `schema` for each name in `common_names`.
fn projection_indices(
    schema: &arrow::datatypes::Schema,
    common_names: &[&str],
) -> eyre::Result<Vec<usize>> {
    common_names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|e| eyre::eyre!("column {name} not found: {e}"))
        })
        .collect()
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

    // Step 2: open readers — lazy streaming for all formats (JSON buffers unavoidably)
    let mut reader1 = open_reader(&args.file1, ft1).await?;
    let mut reader2 = open_reader(&args.file2, ft2).await?;

    // Step 3: compare schemas before consuming any rows
    let schema1 = reader1.schema();
    let schema2 = reader2.schema();

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

    // Step 4: precompute projection indices — only needed when schemas differ
    let needs_projection = !only_in_1.is_empty() || !only_in_2.is_empty();
    let common_names: Vec<&str> = common.iter().map(|(name, _)| name.as_str()).collect();
    let indices1 = needs_projection
        .then(|| projection_indices(&schema1, &common_names))
        .transpose()?;
    let indices2 = needs_projection
        .then(|| projection_indices(&schema2, &common_names))
        .transpose()?;

    // Step 5: interleaved scan with early exit.
    //
    // `running_diffs` is the incremental sum of |c1 - c2| across all keys seen so far.
    // Adding a row from file1 increases it when c1 >= c2 (the row is becoming more
    // "only-in-file1"), and decreases it when c1 < c2 (the row cancels an existing
    // file2-only excess). Symmetric logic applies for file2 rows. This lets us check
    // the total diff count in O(1) per row without scanning the whole map.
    //
    // When we stop early the map is partial, so some diffs may be false positives
    // (rows that would cancel if more of the other file were read). This is acceptable:
    // the caller controls tolerance via --max-diffs.
    let unlimited = args.max_diffs == 0;
    let mut counts: HashMap<String, (i64, i64)> = HashMap::new();
    let mut running_diffs: i64 = 0;
    let mut truncated = false;

    'scan: loop {
        let mut advanced = false;

        if let Some(batch_result) = reader1.next() {
            advanced = true;
            let batch = batch_result?;
            let batch = match &indices1 {
                Some(idx) => batch.project(idx)?,
                None => batch,
            };
            for row in 0..batch.num_rows() {
                let key = row_to_string(&batch, row);
                let entry = counts.entry(key).or_insert((0, 0));
                if entry.0 >= entry.1 {
                    running_diffs += 1;
                } else {
                    running_diffs -= 1;
                }
                entry.0 += 1;
                if !unlimited && running_diffs >= args.max_diffs as i64 {
                    truncated = true;
                    break 'scan;
                }
            }
        }

        if let Some(batch_result) = reader2.next() {
            advanced = true;
            let batch = batch_result?;
            let batch = match &indices2 {
                Some(idx) => batch.project(idx)?,
                None => batch,
            };
            for row in 0..batch.num_rows() {
                let key = row_to_string(&batch, row);
                let entry = counts.entry(key).or_insert((0, 0));
                if entry.1 >= entry.0 {
                    running_diffs += 1;
                } else {
                    running_diffs -= 1;
                }
                entry.1 += 1;
                if !unlimited && running_diffs >= args.max_diffs as i64 {
                    truncated = true;
                    break 'scan;
                }
            }
        }

        if !advanced {
            break;
        }
    }

    // Step 6: report results
    let all_equal = !truncated && counts.values().all(|(c1, c2)| c1 == c2);

    if all_equal {
        let total = counts.values().map(|(c1, _)| *c1).sum::<i64>() as usize;
        let row_label = if total == 1 { "row" } else { "rows" };
        println!("Files are identical ({total} {row_label})");
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

        if truncated {
            println!();
            println!(
                "(output truncated at {} diffs; use --max-diffs to increase the limit)",
                args.max_diffs
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_args(file1: &str, file2: &str) -> DiffArgs {
        DiffArgs {
            file1: file1.to_string(),
            file2: file2.to_string(),
            input: None,
            max_diffs: 100,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_identical_parquet() {
        let result = diff(make_args(
            "fixtures/table.parquet",
            "fixtures/table.parquet",
        ))
        .await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_identical_csv() {
        let result = diff(make_args("fixtures/table.csv", "fixtures/table.csv")).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_format_mismatch() {
        let result = diff(make_args("fixtures/table.csv", "fixtures/table.parquet")).await;
        assert!(result.is_err(), "diff should fail for different formats");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("different formats"),
            "error should mention different formats, got: {err_msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_max_diffs_truncates() {
        // file1 and file2 have 2 total differing rows; max_diffs=1 forces early exit
        let args = DiffArgs {
            max_diffs: 1,
            ..make_args("fixtures/file1.avro", "fixtures/file2.avro")
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_max_diffs_zero_is_unlimited() {
        let args = DiffArgs {
            max_diffs: 0,
            ..make_args("fixtures/file1.avro", "fixtures/file2.avro")
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }
}
