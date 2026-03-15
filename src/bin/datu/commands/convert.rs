use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatchReader;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use clap::Args;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use datu::Error;
use datu::FileType;
use datu::pipeline::Source;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReader;
use datu::pipeline::dataframe::DataFrameReader;
use datu::pipeline::dataframe::write_record_batches_from_reader;
use datu::resolve_input_file_type;
use datu::utils::parse_select_columns;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

/// Arguments for the `datu convert` command.
#[derive(Args)]
pub struct ConvertArgs {
    /// Path to the input file
    pub input_path: String,
    /// Path to the output file
    pub output_path: String,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub input: Option<FileType>,
    #[arg(
        long,
        short = 'O',
        value_parser = clap::value_parser!(FileType),
        help = "Output file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub output: Option<FileType>,
    #[arg(
        long,
        help = "Columns to select. If not specified, all columns will be selected."
    )]
    pub select: Option<Vec<String>>,
    #[arg(long, help = "Maximum number of records to read from the input.")]
    pub limit: Option<usize>,
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "For JSON/YAML: omit keys with null/missing values. Default: true. Use --sparse=false to include default values (e.g. empty string)."
    )]
    pub sparse: bool,
    #[arg(
        long,
        help = "When converting to JSON, format output with indentation and newlines. Ignored for other output formats."
    )]
    pub json_pretty: bool,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV input: whether the first row is a header. Default: true when omitted. Use --input-headers=false for headerless CSV."
    )]
    pub input_headers: Option<bool>,
}

/// Returns true if the format is supported by DataFusion's DataFrame read/write API.
fn is_datafusion_native(file_type: FileType) -> bool {
    matches!(
        file_type,
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json
    )
}

/// Returns the total number of rows from file metadata, if available.
fn get_total_rows(path: &str, file_type: FileType) -> Option<u64> {
    datu::get_total_rows_result(path, file_type)
        .ok()
        .map(|n| n as u64)
}

/// A `RecordBatchReader` wrapper that increments an `indicatif::ProgressBar` by
/// the number of rows in each batch as it is yielded.
struct ProgressRecordBatchReader {
    inner: VecRecordBatchReader,
    progress: ProgressBar,
}

impl Iterator for ProgressRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if let Ok(ref batch) = item {
            self.progress.inc(batch.num_rows() as u64);
        }
        Some(item)
    }
}

impl RecordBatchReader for ProgressRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// Converts between file formats; reads from input and writes to output, optionally selecting columns.
pub async fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    let input_file_type = resolve_input_file_type(args.input, &args.input_path)?;
    let output_file_type = resolve_input_file_type(args.output, &args.output_path)?;

    let use_dataframe_api = is_datafusion_native(input_file_type)
        && (output_file_type == FileType::Parquet || output_file_type == FileType::Csv);

    let total_rows = if !use_dataframe_api || input_file_type == FileType::Parquet {
        get_total_rows(&args.input_path, input_file_type)
    } else {
        None
    };

    let progress = match total_rows {
        Some(total) => {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len} rows ({eta})")
                    .expect("valid template")
                    .progress_chars("=>-"),
            );
            pb.set_message(format!(
                "Converting {} to {}",
                args.input_path, args.output_path
            ));
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner:.cyan} {msg}").expect("valid template"),
            );
            pb.set_message(format!(
                "Converting {} to {}...",
                args.input_path, args.output_path
            ));
            pb.enable_steady_tick(Duration::from_millis(100));
            pb
        }
    };

    let result: Result<(), datu::Error> = if use_dataframe_api {
        convert_via_dataframe_api(&args, input_file_type, output_file_type, &progress).await
    } else {
        convert_via_fallback(&args, input_file_type, output_file_type, &progress).await
    };

    match result {
        Ok(()) => {
            progress.finish_and_clear();
            eprintln!("Converted {} to {}", args.input_path, args.output_path);
            Ok(())
        }
        Err(e) => {
            progress.abandon();
            eprintln!(
                "Failed to convert {} to {}",
                args.input_path, args.output_path
            );
            Err(e.into())
        }
    }
}

/// Streamlined path: DataFusion DataFrame read → select/limit → write. No collect or RecordBatchReader.
async fn convert_via_dataframe_api(
    args: &ConvertArgs,
    input_file_type: FileType,
    output_file_type: FileType,
    _progress: &ProgressBar,
) -> Result<(), datu::Error> {
    let ctx = SessionContext::new();

    let mut df = match input_file_type {
        FileType::Parquet => ctx
            .read_parquet(&args.input_path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?,
        FileType::Avro => ctx
            .read_avro(&args.input_path, AvroReadOptions::default())
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?,
        FileType::Csv => {
            let has_header = args.input_headers.unwrap_or(true);
            ctx.read_csv(
                &args.input_path,
                CsvReadOptions::new().has_header(has_header),
            )
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?
        }
        FileType::Json => ctx
            .read_json(&args.input_path, NdJsonReadOptions::default())
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?,
        _ => {
            return Err(Error::GenericError(
                "Streamlined path only supports Parquet, Avro, CSV, JSON input".to_string(),
            ));
        }
    };

    if let Some(columns) = &args.select {
        let parsed = parse_select_columns(columns);
        if !parsed.is_empty() {
            let col_refs: Vec<&str> = parsed.iter().map(String::as_str).collect();
            df = df
                .select_columns(&col_refs)
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }
    }

    if let Some(n) = args.limit {
        df = df
            .limit(0, Some(n))
            .map_err(|e| Error::GenericError(e.to_string()))?;
    }

    let write_opts = DataFrameWriteOptions::new();

    match output_file_type {
        FileType::Parquet => {
            df.write_parquet(&args.output_path, write_opts, None)
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }
        FileType::Csv => {
            df.write_csv(&args.output_path, write_opts, None)
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }
        FileType::Json => {
            df.write_json(&args.output_path, write_opts, None)
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }
        _ => {
            return Err(Error::GenericError(
                "Streamlined path only supports Parquet, Avro, CSV, JSON output".to_string(),
            ));
        }
    }

    Ok(())
}

/// Fallback path: DataFrameReader → collect → write_record_batches_from_reader.
async fn convert_via_fallback(
    args: &ConvertArgs,
    input_file_type: FileType,
    output_file_type: FileType,
    progress: &ProgressBar,
) -> Result<(), datu::Error> {
    let reader_step = DataFrameReader::new(
        &args.input_path,
        input_file_type,
        args.select.clone(),
        args.limit,
        args.input_headers,
    );

    let mut source = reader_step.execute(()).await?;
    let df = source.get()?;

    let handle = tokio::runtime::Handle::current();
    let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))
        .map_err(|e| Error::GenericError(e.to_string()))?;

    let mut reader = ProgressRecordBatchReader {
        inner: VecRecordBatchReader::new(batches),
        progress: progress.clone(),
    };

    write_record_batches_from_reader(
        &mut reader,
        &args.output_path,
        output_file_type,
        args.sparse,
        args.json_pretty,
    )
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;
    use datu::pipeline::avro;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_avro() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.avro");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.csv");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("userdata5.csv");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/userdata5.avro".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_json() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.json");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_xlsx() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.xlsx");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_orc() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("userdata5.orc");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/userdata5.avro".to_string(),
            output_path,
            input: None,
            output: None,
            select: Some(vec!["id".to_string(), "first_name".to_string()]),
            limit: Some(10),
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_orc_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let orc_path = temp_dir.path().join("userdata5.orc");
        let csv_path = temp_dir.path().join("userdata5.csv");

        // First convert Avro to ORC (select id,first_name for orc-rust type compatibility)
        let orc_args = ConvertArgs {
            input_path: "fixtures/userdata5.avro".to_string(),
            output_path: orc_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            input: None,
            output: None,
            select: Some(vec!["id".to_string(), "first_name".to_string()]),
            limit: Some(10),
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };
        convert(orc_args).await.expect("Avro to ORC failed");

        // Then convert ORC to CSV
        let csv_args = ConvertArgs {
            input_path: orc_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            output_path: csv_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };
        let result = convert(csv_args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(csv_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_yaml() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.yaml");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path,
            input: None,
            output: None,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_with_select() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.csv");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path: output_path.clone(),
            input: None,
            output: None,
            select: Some(vec!["two".to_string(), "four".to_string()]),
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(
            result.is_ok(),
            "Convert with select failed: {:?}",
            result.err()
        );
        assert!(output_path_buf.exists(), "Output file was not created");

        let mut reader = csv::Reader::from_path(&output_path).expect("Failed to open output CSV");
        let headers: Vec<String> = reader
            .headers()
            .expect("Failed to read CSV headers")
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            headers,
            vec!["two".to_string(), "four".to_string()],
            "CSV header columns do not match selected columns"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_select_one_two_to_avro() {
        // Equivalent to: read("fixtures/table.parquet") |> select(:one, :two) |> write("${TMP}/table.avro")
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path_buf = temp_dir.path().join("table.avro");
        let output_path = output_path_buf
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input_path: "fixtures/table.parquet".to_string(),
            output_path: output_path.clone(),
            input: None,
            output: None,
            select: Some(vec!["one".to_string(), "two".to_string()]),
            limit: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        };

        let result = convert(args).await;
        assert!(
            result.is_ok(),
            "Convert parquet select one,two to avro failed: {:?}",
            result.err()
        );
        assert!(output_path_buf.exists(), "Output file was not created");

        let read_args = datu::pipeline::ReadArgs {
            path: output_path,
            limit: None,
            offset: None,
            csv_has_header: None,
        };
        let reader = avro::read_avro(&read_args).expect("Failed to read output Avro");
        let field_names: Vec<String> = reader
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(
            field_names,
            vec!["one".to_string(), "two".to_string()],
            "Avro schema columns do not match selected columns"
        );
    }
}
