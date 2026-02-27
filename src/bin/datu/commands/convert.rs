use anyhow::Result;
use anyhow::bail;
use clap::Args;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;
use datu::FileType;
use datu::pipeline::VecRecordBatchReader;
use datu::pipeline::avro;
use datu::pipeline::display;
use datu::pipeline::orc;
use datu::pipeline::xlsx;
use datu::utils::parse_select_columns;

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

    let df = read_to_dataframe(&args.input, input_file_type, &args.select, args.limit).await?;

    write_dataframe(
        df,
        &args.output,
        output_file_type,
        args.sparse,
        args.json_pretty,
    )
    .await?;

    Ok(())
}

/// Reads input file into a DataFusion DataFrame with optional column selection and limit.
async fn read_to_dataframe(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
) -> Result<datafusion::dataframe::DataFrame> {
    let ctx = SessionContext::new();

    let mut df = match input_file_type {
        FileType::Parquet => ctx
            .read_parquet(input_path, ParquetReadOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        FileType::Avro => ctx
            .read_avro(input_path, AvroReadOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        FileType::Orc => {
            let batches = read_orc_to_batches(input_path)?;
            if batches.is_empty() {
                bail!("ORC file is empty or could not be read");
            }
            ctx.read_batches(batches)
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
        _ => bail!("Only Parquet, Avro, and ORC are supported as input file types"),
    };

    if let Some(columns) = select {
        let parsed = parse_select_columns(columns);
        if !parsed.is_empty() {
            let col_refs: Vec<&str> = parsed.iter().map(String::as_str).collect();
            df = df
                .select_columns(&col_refs)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    if let Some(n) = limit {
        df = df.limit(0, Some(n)).map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    Ok(df)
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion in read_to_dataframe.
fn read_orc_to_batches(path: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    use datu::pipeline::ReadArgs;

    let args = ReadArgs {
        path: path.to_string(),
        limit: None,
        offset: None,
    };
    let reader = orc::read_orc(&args)?;
    let batches: Vec<arrow::record_batch::RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(batches)
}

/// Writes a DataFusion DataFrame to the output file.
async fn write_dataframe(
    df: datafusion::dataframe::DataFrame,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> Result<()> {
    if output_file_type != FileType::Json && json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }

    match output_file_type {
        FileType::Parquet => {
            df.write_parquet(output_path, DataFrameWriteOptions::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        FileType::Csv => {
            df.write_csv(output_path, DataFrameWriteOptions::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        FileType::Json => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            let file = std::fs::File::create(output_path).map_err(anyhow::Error::from)?;
            if json_pretty {
                display::write_record_batches_as_json_pretty(&mut reader, file, sparse)?;
            } else {
                display::write_record_batches_as_json(&mut reader, file, sparse)?;
            }
        }
        FileType::Yaml => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            let file = std::fs::File::create(output_path).map_err(anyhow::Error::from)?;
            display::write_record_batches_as_yaml(&mut reader, file, sparse)?;
        }
        FileType::Avro => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            avro::write_record_batches(output_path, &mut reader)?;
        }
        FileType::Orc => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            orc::write_record_batches(output_path, &mut reader)?;
        }
        FileType::Xlsx => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            xlsx::write_record_batches(output_path, &mut reader)?;
        }
    }

    Ok(())
}

/// Reads input into record batches for use by REPL and other callers that need RecordBatchReaderSource.
/// Uses DataFusion for Parquet and Avro; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let df = read_to_dataframe(input_path, input_file_type, select, limit).await?;
    df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))
}

/// Writes record batches to output file. Used by REPL.
pub async fn write_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    write_dataframe(df, output_path, output_file_type, sparse, json_pretty).await
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;

    use super::*;

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
