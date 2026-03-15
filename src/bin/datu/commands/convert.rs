use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatchReader;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use clap::Args;
use datu::Error;
use datu::FileType;
use datu::pipeline::Source;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReader;
use datu::pipeline::avro;
use datu::pipeline::dataframe::DataFrameReader;
use datu::pipeline::display;
use datu::pipeline::orc;
use datu::pipeline::parquet as parquet_writer;
use datu::pipeline::xlsx;
use datu::resolve_input_file_type;
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

    let total_rows = get_total_rows(&args.input_path, input_file_type);

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

    let reader_step = DataFrameReader::new(
        &args.input_path,
        input_file_type,
        args.select,
        args.limit,
        args.input_headers,
    );

    let result: Result<(), datu::Error> = async {
        let mut source = reader_step.execute(()).await?;
        let df = source.get()?;

        if output_file_type != FileType::Json && args.json_pretty {
            eprintln!("Warning: --json-pretty is only supported when converting to JSON");
        }

        let handle = tokio::runtime::Handle::current();
        let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))
            .map_err(|e| Error::GenericError(e.to_string()))?;

        let mut reader = ProgressRecordBatchReader {
            inner: VecRecordBatchReader::new(batches),
            progress: progress.clone(),
        };

        let output_path = &args.output_path;

        match output_file_type {
            FileType::Parquet => {
                parquet_writer::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Csv => {
                datu::pipeline::csv::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Json => {
                let file = File::create(output_path).map_err(Error::IoError)?;
                if args.json_pretty {
                    display::write_record_batches_as_json_pretty(&mut reader, file, args.sparse)?;
                } else {
                    display::write_record_batches_as_json(&mut reader, file, args.sparse)?;
                }
            }
            FileType::Yaml => {
                let file = File::create(output_path).map_err(Error::IoError)?;
                display::write_record_batches_as_yaml(&mut reader, file, args.sparse)?;
            }
            FileType::Avro => {
                avro::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Orc => {
                orc::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Xlsx => {
                xlsx::write_record_batches(output_path, &mut reader)?;
            }
        }

        Ok(())
    }
    .await;

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
