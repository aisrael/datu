//! `datu diff` — compare two data files row-by-row

use std::collections::HashMap;
use std::str::FromStr;

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
use serde::Serialize;

/// Output format for the `datu diff` command.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DiffOutputFormat {
    #[default]
    Text,
    Json,
}

impl TryFrom<&str> for DiffOutputFormat {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "json" => Ok(DiffOutputFormat::Json),
            _ => Err(format!("unknown output type '{s}', expected json")),
        }
    }
}

impl FromStr for DiffOutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

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
    pub limit: usize,
    #[arg(long = "max-diffs", alias = "max-diff", hide = true)]
    pub max_diffs: Option<usize>,
    #[arg(long, help = "Emit diff results as JSON")]
    pub json: bool,
    #[arg(
        long,
        short,
        value_parser = clap::value_parser!(DiffOutputFormat),
        help = "Output format: json"
    )]
    pub output: Option<DiffOutputFormat>,
}

impl DiffArgs {
    fn effective_limit(&self) -> usize {
        if let Some(max_diffs) = self.max_diffs {
            eprintln!(
                "Warning: `--max-diffs` is deprecated; use `--limit` instead. `--max-diffs` may be removed in a future version."
            );
            max_diffs
        } else {
            self.limit
        }
    }

    fn output_format(&self) -> DiffOutputFormat {
        if self.json {
            return DiffOutputFormat::Json;
        }
        self.output.unwrap_or(DiffOutputFormat::Text)
    }
}

/// Aggregated diff results used for text and JSON rendering.
struct DiffReport {
    file1: String,
    file2: String,
    identical: bool,
    row_count: Option<usize>,
    columns: Vec<String>,
    schema_only_in_file1: Vec<(String, String)>,
    schema_only_in_file2: Vec<(String, String)>,
    only_in_file1: Vec<String>,
    only_in_file2: Vec<String>,
    truncated: bool,
    limit: usize,
}

#[derive(Serialize)]
struct SchemaColumnJson {
    name: String,
    data_type: String,
}

#[derive(Serialize)]
struct SchemaDiffJson {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    only_in_file1: Vec<SchemaColumnJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    only_in_file2: Vec<SchemaColumnJson>,
}

#[derive(Serialize)]
struct DiffJsonOutput {
    identical: bool,
    file1: String,
    file2: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<SchemaDiffJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    only_in_file1: Vec<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    only_in_file2: Vec<HashMap<String, String>>,
    #[serde(skip_serializing_if = "is_false")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn schema_columns(columns: &[(String, String)]) -> Vec<SchemaColumnJson> {
    columns
        .iter()
        .map(|(name, data_type)| SchemaColumnJson {
            name: name.clone(),
            data_type: data_type.clone(),
        })
        .collect()
}

fn row_strings_to_objects(columns: &[String], rows: &[String]) -> Vec<HashMap<String, String>> {
    rows.iter()
        .map(|row| {
            let values: Vec<&str> = row.split('\t').collect();
            columns
                .iter()
                .zip(values)
                .map(|(col, val)| (col.clone(), val.to_string()))
                .collect()
        })
        .collect()
}

fn print_schema_diff_text(
    file1: &str,
    file2: &str,
    only_in_file1: &[(String, String)],
    only_in_file2: &[(String, String)],
) {
    eprintln!("Schema differences:");
    if !only_in_file1.is_empty() {
        eprintln!("  Only in {file1}:");
        for (name, dt) in only_in_file1 {
            eprintln!("    {name}: {dt}");
        }
    }
    if !only_in_file2.is_empty() {
        eprintln!("  Only in {file2}:");
        for (name, dt) in only_in_file2 {
            eprintln!("    {name}: {dt}");
        }
    }
}

fn print_diff_text(report: &DiffReport) {
    if !report.schema_only_in_file1.is_empty() || !report.schema_only_in_file2.is_empty() {
        print_schema_diff_text(
            &report.file1,
            &report.file2,
            &report.schema_only_in_file1,
            &report.schema_only_in_file2,
        );
    }

    if report.identical {
        let total = report.row_count.unwrap_or(0);
        let row_label = if total == 1 { "row" } else { "rows" };
        println!("Files are identical ({total} {row_label})");
        return;
    }

    let header = report.columns.join("\t");

    if !report.only_in_file1.is_empty() {
        let count = report.only_in_file1.len();
        let row_label = if count == 1 { "row" } else { "rows" };
        println!("Only in {} ({count} {row_label}):", report.file1);
        println!("  {header}");
        for row_str in &report.only_in_file1 {
            println!("  {row_str}");
        }
    }

    if !report.only_in_file1.is_empty() && !report.only_in_file2.is_empty() {
        println!();
    }

    if !report.only_in_file2.is_empty() {
        let count = report.only_in_file2.len();
        let row_label = if count == 1 { "row" } else { "rows" };
        println!("Only in {} ({count} {row_label}):", report.file2);
        println!("  {header}");
        for row_str in &report.only_in_file2 {
            println!("  {row_str}");
        }
    }

    if report.truncated {
        println!();
        println!(
            "(output truncated at {} diffs; use --limit to increase the limit)",
            report.limit
        );
    }
}

fn diff_json_string(report: &DiffReport) -> eyre::Result<String> {
    let schema = if report.schema_only_in_file1.is_empty() && report.schema_only_in_file2.is_empty()
    {
        None
    } else {
        Some(SchemaDiffJson {
            only_in_file1: schema_columns(&report.schema_only_in_file1),
            only_in_file2: schema_columns(&report.schema_only_in_file2),
        })
    };

    let output = if report.identical {
        DiffJsonOutput {
            identical: true,
            file1: report.file1.clone(),
            file2: report.file2.clone(),
            row_count: report.row_count,
            columns: None,
            schema: None,
            only_in_file1: Vec::new(),
            only_in_file2: Vec::new(),
            truncated: false,
            limit: None,
        }
    } else {
        DiffJsonOutput {
            identical: false,
            file1: report.file1.clone(),
            file2: report.file2.clone(),
            row_count: None,
            columns: Some(report.columns.clone()),
            schema,
            only_in_file1: row_strings_to_objects(&report.columns, &report.only_in_file1),
            only_in_file2: row_strings_to_objects(&report.columns, &report.only_in_file2),
            truncated: report.truncated,
            limit: Some(report.limit),
        }
    };

    Ok(serde_json::to_string(&output)?)
}

fn print_diff_json(report: &DiffReport) -> eyre::Result<()> {
    println!("{}", diff_json_string(report)?);
    Ok(())
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

/// Schema columns present in only one of the two files.
struct SchemaOnlyColumns {
    file1: Vec<(String, String)>,
    file2: Vec<(String, String)>,
}

fn build_diff_report(
    file1: &str,
    file2: &str,
    schema_only: SchemaOnlyColumns,
    common_names: &[&str],
    counts: &HashMap<String, (i64, i64)>,
    truncated: bool,
    limit: usize,
) -> DiffReport {
    let SchemaOnlyColumns {
        file1: schema_only_in_file1,
        file2: schema_only_in_file2,
    } = schema_only;
    let columns: Vec<String> = common_names
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    let all_equal = !truncated && counts.values().all(|(c1, c2)| c1 == c2);

    if all_equal {
        let total = counts.values().map(|(c1, _)| *c1).sum::<i64>() as usize;
        return DiffReport {
            file1: file1.to_string(),
            file2: file2.to_string(),
            identical: true,
            row_count: Some(total),
            columns,
            schema_only_in_file1,
            schema_only_in_file2,
            only_in_file1: Vec::new(),
            only_in_file2: Vec::new(),
            truncated: false,
            limit,
        };
    }

    let mut only_in_file1: Vec<String> = Vec::new();
    let mut only_in_file2: Vec<String> = Vec::new();

    for (row_str, (c1, c2)) in counts {
        if c1 > c2 {
            for _ in 0..(c1 - c2) {
                only_in_file1.push(row_str.clone());
            }
        } else if c2 > c1 {
            for _ in 0..(c2 - c1) {
                only_in_file2.push(row_str.clone());
            }
        }
    }

    only_in_file1.sort();
    only_in_file2.sort();

    DiffReport {
        file1: file1.to_string(),
        file2: file2.to_string(),
        identical: false,
        row_count: None,
        columns,
        schema_only_in_file1,
        schema_only_in_file2,
        only_in_file1,
        only_in_file2,
        truncated,
        limit,
    }
}

/// The `datu diff` command: compares two data files and reports row-level differences.
pub async fn diff(args: DiffArgs) -> eyre::Result<()> {
    let output_format = args.output_format();

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

    let schema_only_in_file1: Vec<(String, String)> = cols1
        .iter()
        .filter(|c| !set2.contains(c))
        .map(|(name, dt)| (name.clone(), dt.to_string()))
        .collect();
    let schema_only_in_file2: Vec<(String, String)> = cols2
        .iter()
        .filter(|c| !set1.contains(c))
        .map(|(name, dt)| (name.clone(), dt.to_string()))
        .collect();
    let common: Vec<_> = cols1.iter().filter(|c| set2.contains(c)).collect();

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
    let needs_projection = !schema_only_in_file1.is_empty() || !schema_only_in_file2.is_empty();
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
    // the caller controls tolerance via --limit.
    let limit = args.effective_limit();
    let unlimited = limit == 0;
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
                if !unlimited && running_diffs >= limit as i64 {
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
                if !unlimited && running_diffs >= limit as i64 {
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
    let report = build_diff_report(
        &args.file1,
        &args.file2,
        SchemaOnlyColumns {
            file1: schema_only_in_file1,
            file2: schema_only_in_file2,
        },
        &common_names,
        &counts,
        truncated,
        limit,
    );

    match output_format {
        DiffOutputFormat::Text => print_diff_text(&report),
        DiffOutputFormat::Json => print_diff_json(&report)?,
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
            limit: 100,
            max_diffs: None,
            json: false,
            output: None,
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
    async fn test_diff_limit_truncates() {
        // file1 and file2 have 2 total differing rows; limit=1 forces early exit
        let args = DiffArgs {
            limit: 1,
            ..make_args("fixtures/file1.avro", "fixtures/file2.avro")
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_diff_limit_zero_is_unlimited() {
        let args = DiffArgs {
            limit: 0,
            ..make_args("fixtures/file1.avro", "fixtures/file2.avro")
        };
        let result = diff(args).await;
        assert!(result.is_ok(), "diff failed: {:?}", result.err());
    }

    #[test]
    fn test_effective_limit_uses_limit_by_default() {
        let args = make_args("a", "b");
        assert_eq!(args.effective_limit(), 100);
    }

    #[test]
    fn test_effective_limit_prefers_deprecated_max_diffs() {
        let args = DiffArgs {
            limit: 100,
            max_diffs: Some(5),
            ..make_args("a", "b")
        };
        assert_eq!(args.effective_limit(), 5);
    }

    #[test]
    fn test_output_format_defaults_to_text() {
        let args = make_args("a", "b");
        assert_eq!(args.output_format(), DiffOutputFormat::Text);
    }

    #[test]
    fn test_output_format_json_flag() {
        let args = DiffArgs {
            json: true,
            ..make_args("a", "b")
        };
        assert_eq!(args.output_format(), DiffOutputFormat::Json);
    }

    #[test]
    fn test_output_format_output_option() {
        let args = DiffArgs {
            output: Some(DiffOutputFormat::Json),
            ..make_args("a", "b")
        };
        assert_eq!(args.output_format(), DiffOutputFormat::Json);
    }

    #[test]
    fn test_diff_json_string_identical() {
        let report = DiffReport {
            file1: "a.avro".to_string(),
            file2: "a.avro".to_string(),
            identical: true,
            row_count: Some(4),
            columns: vec!["id".to_string(), "name".to_string()],
            schema_only_in_file1: Vec::new(),
            schema_only_in_file2: Vec::new(),
            only_in_file1: Vec::new(),
            only_in_file2: Vec::new(),
            truncated: false,
            limit: 100,
        };

        let json = diff_json_string(&report).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["identical"], true);
        assert_eq!(value["row_count"], 4);
        assert_eq!(value["file1"], "a.avro");
        assert_eq!(value["file2"], "a.avro");
    }

    #[test]
    fn test_diff_json_string_differing() {
        let report = DiffReport {
            file1: "file1.avro".to_string(),
            file2: "file2.avro".to_string(),
            identical: false,
            row_count: None,
            columns: vec!["id".to_string(), "name".to_string()],
            schema_only_in_file1: Vec::new(),
            schema_only_in_file2: vec![("email".to_string(), "Utf8".to_string())],
            only_in_file1: vec!["1\tfoo".to_string()],
            only_in_file2: vec!["4\tfizz".to_string()],
            truncated: false,
            limit: 100,
        };

        let json = diff_json_string(&report).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["identical"], false);
        assert_eq!(value["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(value["schema"]["only_in_file2"][0]["name"], "email");
        assert_eq!(value["only_in_file1"][0]["name"], "foo");
        assert_eq!(value["only_in_file2"][0]["name"], "fizz");
        assert_eq!(value["limit"], 100);
    }
}
