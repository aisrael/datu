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

/// convert command arguments
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
        help = "For JSON/YAML: omit keys with null/missing values. If false, output default values (e.g. empty string)."
    )]
    pub sparse: bool,
    #[arg(
        long,
        help = "When converting to JSON, format output with indentation and newlines. Ignored for other output formats."
    )]
    pub json_pretty: bool,
}

/// convert command implementation
pub fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let output_file_type: FileType = args.output.as_str().try_into()?;

    println!("Converting {} to {}", args.input, args.output);

    let mut reader_step: Box<dyn RecordBatchReaderSource> =
        get_reader_step(input_file_type, &args)?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step: Box<dyn RecordBatchReaderSource> = Box::new(SelectColumnsStep {
            prev: reader_step,
            columns,
        });
        reader_step = select_step;
    }
    execute_writer(reader_step, output_file_type, &args)?;

    Ok(())
}

fn get_reader_step(
    input_file_type: FileType,
    args: &ConvertArgs,
) -> Result<Box<dyn RecordBatchReaderSource>> {
    let reader: Box<dyn RecordBatchReaderSource> = match input_file_type {
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

fn execute_writer(
    prev: Box<dyn RecordBatchReaderSource>,
    output_file_type: FileType,
    args: &ConvertArgs,
) -> Result<()> {
    if output_file_type != FileType::Json && args.json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }
    match output_file_type {
        FileType::Csv => {
            let writer = WriteCsvStep {
                prev,
                args: WriteArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Avro => {
            let writer = WriteAvroStep {
                prev,
                args: WriteArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Parquet => {
            let writer = WriteParquetStep {
                prev,
                args: WriteArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Orc => {
            let writer = WriteOrcStep {
                prev,
                args: WriteArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Json => {
            let writer = WriteJsonStep {
                prev,
                args: WriteJsonArgs {
                    path: args.output.clone(),
                    sparse: args.sparse,
                    pretty: args.json_pretty,
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Xlsx => {
            let writer = WriteXlsxStep {
                prev,
                args: WriteArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Yaml => {
            let writer = WriteYamlStep {
                prev,
                args: WriteYamlArgs {
                    path: args.output.clone(),
                    sparse: args.sparse,
                },
            };
            writer.execute()?;
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
