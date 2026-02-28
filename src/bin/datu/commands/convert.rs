use clap::Args;
use datu::FileType;
use datu::cli::convert::DataFrameReader;
use datu::cli::convert::DataFrameWriter;
use datu::cli::convert::read_dataframe;
use datu::cli::convert::write_dataframe;
use datu::pipeline::Step;

/// Arguments for the `datu convert` command.
#[derive(Args)]
pub struct ConvertArgs {
    pub input: String,
    pub output: String,
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
}

/// Converts between file formats; reads from input and writes to output, optionally selecting columns.
pub async fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let output_file_type: FileType = args.output.as_str().try_into()?;

    println!("Converting {} to {}", args.input, args.output);

    let reader_step = DataFrameReader::new(&args.input, input_file_type, args.select, args.limit);

    let writer_step =
        DataFrameWriter::new(args.output, output_file_type, args.sparse, args.json_pretty);
    let source = reader_step.execute(())?;
    writer_step.execute(source)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;
    use datu::pipeline::avro;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_avro() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.avro");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.csv");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("userdata5.csv");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/userdata5.avro".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_json() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.json");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_xlsx() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.xlsx");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_avro_to_orc() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("userdata5.orc");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/userdata5.avro".to_string(),
            output,
            select: Some(vec!["id".to_string(), "first_name".to_string()]),
            limit: Some(10),
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_orc_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let orc_path = temp_dir.path().join("userdata5.orc");
        let csv_path = temp_dir.path().join("userdata5.csv");

        // First convert Avro to ORC (select id,first_name for orc-rust type compatibility)
        let orc_args = ConvertArgs {
            input: "fixtures/userdata5.avro".to_string(),
            output: orc_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            select: Some(vec!["id".to_string(), "first_name".to_string()]),
            limit: Some(10),
            sparse: true,
            json_pretty: false,
        };
        convert(orc_args).await.expect("Avro to ORC failed");

        // Then convert ORC to CSV
        let csv_args = ConvertArgs {
            input: orc_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            output: csv_path
                .to_str()
                .expect("Failed to convert path to string")
                .to_string(),
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };
        let result = convert(csv_args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(csv_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_parquet_to_yaml() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.yaml");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: None,
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_convert_with_select() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.csv");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: Some(vec!["two".to_string(), "four".to_string()]),
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(
            result.is_ok(),
            "Convert with select failed: {:?}",
            result.err()
        );
        assert!(output_path.exists(), "Output file was not created");

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
        let output_path = temp_dir.path().join("table.avro");
        let output = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let args = ConvertArgs {
            input: "fixtures/table.parquet".to_string(),
            output,
            select: Some(vec!["one".to_string(), "two".to_string()]),
            limit: None,
            sparse: true,
            json_pretty: false,
        };

        let result = convert(args).await;
        assert!(
            result.is_ok(),
            "Convert parquet select one,two to avro failed: {:?}",
            result.err()
        );
        assert!(output_path.exists(), "Output file was not created");

        let read_args = datu::pipeline::ReadArgs {
            path: output_path.to_str().expect("path").to_string(),
            limit: None,
            offset: None,
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
