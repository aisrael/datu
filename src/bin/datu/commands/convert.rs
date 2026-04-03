use std::time::Duration;

use clap::Args;
use datu::FileType;
use datu::pipeline::PipelineBuilder;
use datu::pipeline::SelectSpec;
use datu::resolve_file_type;
use eyre::Result;
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

/// Converts between file formats; reads from input and writes to output, optionally selecting columns.
pub async fn convert(args: ConvertArgs) -> eyre::Result<()> {
    let progress = create_progress_bar(&args)?;

    let select_spec = SelectSpec::from_cli_args(&args.select);

    let mut builder = PipelineBuilder::new();
    builder
        .read(&args.input_path)
        .write(&args.output_path)
        .input_type(args.input)
        .output_type(args.output)
        .csv_has_header(args.input_headers)
        .sparse(args.sparse)
        .json_pretty(args.json_pretty)
        .progress(Some(progress.clone()));

    if let Some(spec) = select_spec {
        builder.select_spec(spec);
    }

    if let Some(n) = args.limit {
        builder.head(n);
    }

    let result = builder.build().and_then(|mut pipeline| pipeline.execute());

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

/// Returns the total number of rows from file metadata, if available.
fn get_total_rows(path: &str, file_type: FileType) -> Option<u64> {
    datu::get_total_rows_result(path, file_type)
        .ok()
        .map(|n| n as u64)
}

/// Creates a progress bar for the convert command.
fn create_progress_bar(args: &ConvertArgs) -> Result<ProgressBar> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    let output_file_type = resolve_file_type(args.output, &args.output_path)?;

    let use_streaming_write = input_file_type.supports_datafusion_file_read()
        && (output_file_type == FileType::Parquet || output_file_type == FileType::Csv);

    let total_rows = if !use_streaming_write || input_file_type == FileType::Parquet {
        get_total_rows(&args.input_path, input_file_type)
    } else {
        None
    };

    Ok(match total_rows {
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
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use arrow::array::RecordBatchReader;
    use datu::FileType;
    use datu::pipeline::avro;
    use datu::pipeline::read::ReadArgs;

    use super::*;

    /// Creates a temporary output path for testing.
    fn temp_output_path(temp: &tempfile::TempDir, file_name: &str) -> (PathBuf, String) {
        let path = temp.path().join(file_name);
        let s = path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();
        (path, s)
    }

    /// Creates a ConvertArgs struct for testing.
    fn convert_args_for_tests(
        input_path: impl Into<String>,
        output_path: impl Into<String>,
        select: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> ConvertArgs {
        ConvertArgs {
            input_path: input_path.into(),
            output_path: output_path.into(),
            input: None,
            output: None,
            select,
            limit,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_avro() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.avro");
        let args = convert_args_for_tests("fixtures/table.parquet", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.csv");
        let args = convert_args_for_tests("fixtures/table.parquet", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "userdata5.csv");
        let args = convert_args_for_tests("fixtures/userdata5.avro", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_json() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.json");
        let args = convert_args_for_tests("fixtures/table.parquet", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_xlsx() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.xlsx");
        let args = convert_args_for_tests("fixtures/table.parquet", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_orc() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "userdata5.orc");
        let args = convert_args_for_tests(
            "fixtures/userdata5.avro",
            output_path,
            Some(vec!["id".to_string(), "first_name".to_string()]),
            Some(10),
        );

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_orc_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (_orc_path, orc_path_str) = temp_output_path(&temp_dir, "userdata5.orc");
        let (csv_path, csv_path_str) = temp_output_path(&temp_dir, "userdata5.csv");

        // First convert Avro to ORC (select id,first_name for orc-rust type compatibility)
        let orc_args = convert_args_for_tests(
            "fixtures/userdata5.avro",
            orc_path_str.clone(),
            Some(vec!["id".to_string(), "first_name".to_string()]),
            Some(10),
        );
        convert(orc_args).await.expect("Avro to ORC failed");

        // Then convert ORC to CSV
        let csv_args = convert_args_for_tests(orc_path_str, csv_path_str, None, None);
        let result = convert(csv_args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(csv_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_yaml() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.yaml");
        let args = convert_args_for_tests("fixtures/table.parquet", output_path, None, None);

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path_buf.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_with_select() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.csv");
        let args = convert_args_for_tests(
            "fixtures/table.parquet",
            output_path.clone(),
            Some(vec!["two".to_string(), "four".to_string()]),
            None,
        );

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
        let (output_path_buf, output_path) = temp_output_path(&temp_dir, "table.avro");
        let args = convert_args_for_tests(
            "fixtures/table.parquet",
            output_path.clone(),
            Some(vec!["one".to_string(), "two".to_string()]),
            None,
        );

        let result = convert(args).await;
        assert!(
            result.is_ok(),
            "Convert parquet select one,two to avro failed: {:?}",
            result.err()
        );
        assert!(output_path_buf.exists(), "Output file was not created");

        let read_args = ReadArgs::new(output_path, FileType::Avro);
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
