use anyhow::Result;
use anyhow::bail;
use dtfu::FileType;
use dtfu::cli::ConvertArgs;
use dtfu::pipeline::RecordBatchReaderSource;
use dtfu::pipeline::Step;
use dtfu::pipeline::avro::ReadAvroArgs;
use dtfu::pipeline::avro::ReadAvroStep;
use dtfu::pipeline::avro::WriteAvroArgs;
use dtfu::pipeline::avro::WriteAvroStep;
use dtfu::pipeline::csv::WriteCsvArgs;
use dtfu::pipeline::csv::WriteCsvStep;
use dtfu::pipeline::parquet::ReadParquetArgs;
use dtfu::pipeline::parquet::ReadParquetStep;
use dtfu::pipeline::parquet::WriteParquetArgs;
use dtfu::pipeline::parquet::WriteParquetStep;
use dtfu::pipeline::record_batch_filter::SelectColumnsStep;
use dtfu::utils::parse_select_columns;

/// convert command implementation
pub fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    println!("Converting {} to {}", args.input, args.output);

    let input_file_type: FileType = args.input.as_str().try_into()?;
    let output_file_type: FileType = args.output.as_str().try_into()?;

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
            args: ReadParquetArgs {
                path: args.input.clone(),
                limit: args.limit,
                offset: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadAvroArgs {
                path: args.input.clone(),
                limit: args.limit,
            },
        }),
        _ => bail!("Only Parquet and Avro are supported as input file types"),
    };
    Ok(reader)
}

fn execute_writer(
    prev: Box<dyn RecordBatchReaderSource>,
    output_file_type: FileType,
    args: &ConvertArgs,
) -> Result<()> {
    match output_file_type {
        FileType::Csv => {
            let writer = WriteCsvStep {
                prev,
                args: WriteCsvArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Avro => {
            let writer = WriteAvroStep {
                prev,
                args: WriteAvroArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        FileType::Parquet => {
            let writer = WriteParquetStep {
                prev,
                args: WriteParquetArgs {
                    path: args.output.clone(),
                },
            };
            writer.execute()?;
            Ok(())
        }
        _ => bail!("Only CSV, Avro and Parquet are supported as output file types"),
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
