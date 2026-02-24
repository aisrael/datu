use anyhow::Result;
use anyhow::bail;
use clap::Args;
use datu::FileType;
use datu::pipeline::ReadArgs;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::WriteArgs;
use datu::pipeline::WriteJsonArgs;
use datu::pipeline::WriteYamlArgs;
use datu::pipeline::avro::ReadAvroStep;
use datu::pipeline::avro::WriteAvroStep;
use datu::pipeline::csv::WriteCsvStep;
use datu::pipeline::json::WriteJsonStep;
use datu::pipeline::orc::ReadOrcStep;
use datu::pipeline::orc::WriteOrcStep;
use datu::pipeline::parquet::ReadParquetStep;
use datu::pipeline::parquet::WriteParquetStep;
use datu::pipeline::record_batch_filter::SelectColumnsStep;
use datu::pipeline::xlsx::WriteXlsxStep;
use datu::pipeline::yaml::WriteYamlStep;
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

/// Converts between file formats; reads from input and writes to output.
pub fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let output_file_type: FileType = args.output.as_str().try_into()?;

    println!("Converting {} to {}", args.input, args.output);

    let mut reader_step: RecordBatchReaderSource = get_reader_step(input_file_type, &args)?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step)?;
    }
    let sparse = args.sparse;
    get_writer_step(reader_step, output_file_type, &args, sparse)
}

/// Builds a record batch reader source for the given input file type and convert args.
pub fn get_reader_step(
    input_file_type: FileType,
    args: &ConvertArgs,
) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match input_file_type {
        FileType::Parquet => Box::new(ReadParquetStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: args.limit,
                offset: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: args.limit,
                offset: None,
            },
        }),
        FileType::Orc => Box::new(ReadOrcStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: args.limit,
                offset: None,
            },
        }),
        _ => bail!("Only Parquet, Avro, and ORC are supported as input file types"),
    };
    Ok(reader)
}

impl From<&ConvertArgs> for WriteArgs {
    fn from(args: &ConvertArgs) -> Self {
        WriteArgs {
            path: args.output.clone(),
        }
    }
}

/// Builds a writer step from the reader step for the given output format and executes it.
fn get_writer_step(
    reader_step: RecordBatchReaderSource,
    output_file_type: FileType,
    args: &ConvertArgs,
    sparse: bool,
) -> Result<()> {
    if output_file_type != FileType::Json && args.json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }
    match output_file_type {
        FileType::Csv => {
            let writer_step = WriteCsvStep {
                args: args.into(),
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Avro => {
            let writer_step = WriteAvroStep {
                args: args.into(),
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Parquet => {
            let writer_step = WriteParquetStep {
                args: args.into(),
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Orc => {
            let writer_step = WriteOrcStep {
                args: args.into(),
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Json => {
            let writer_step = WriteJsonStep {
                args: WriteJsonArgs {
                    path: args.output.clone(),
                    sparse,
                    pretty: args.json_pretty,
                },
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Xlsx => {
            let writer_step = WriteXlsxStep {
                args: args.into(),
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
        FileType::Yaml => {
            let writer_step = WriteYamlStep {
                args: WriteYamlArgs {
                    path: args.output.clone(),
                    sparse,
                },
                source: reader_step,
            };
            writer_step.execute(())?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_parquet_to_avro() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_parquet_to_csv() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_avro_to_csv() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_parquet_to_json() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_parquet_to_xlsx() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_avro_to_orc() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_orc_to_csv() {
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
        convert(orc_args).expect("Avro to ORC failed");

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
        let result = convert(csv_args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(csv_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_parquet_to_yaml() {
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

        let result = convert(args);
        assert!(result.is_ok(), "Convert failed: {:?}", result.err());
        assert!(output_path.exists(), "Output file was not created");
    }

    #[test]
    fn test_convert_with_select() {
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

        let result = convert(args);
        assert!(
            result.is_ok(),
            "Convert with select failed: {:?}",
            result.err()
        );
        assert!(output_path.exists(), "Output file was not created");
    }
}
