//! `datu count` - return the number of rows in a Parquet, Avro, or ORC file

use anyhow::Result;
use anyhow::bail;
use datu::FileType;
use datu::cli::CountArgs;
use datu::pipeline::ReadArgs;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::avro::ReadAvroStep;
use datu::pipeline::csv::ReadCsvStep;
use datu::pipeline::orc::ReadOrcStep;
use datu::pipeline::parquet::ReadParquetStep;
use datu::resolve_input_file_type;

/// The `datu count` command
pub async fn count(args: CountArgs) -> anyhow::Result<()> {
    let file_type = resolve_input_file_type(args.input, &args.input_path)?;
    let mut reader_step: RecordBatchReaderSource = get_reader_step(file_type, &args)?;

    let reader = reader_step.get()?;
    let mut total: usize = 0;
    for batch in reader {
        let batch = batch.map_err(anyhow::Error::from)?;
        total += batch.num_rows();
    }

    println!("{total}");
    Ok(())
}

fn get_reader_step(file_type: FileType, args: &CountArgs) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match file_type {
        FileType::Parquet => Box::new(ReadParquetStep {
            args: ReadArgs {
                path: args.input_path.clone(),
                limit: None,
                offset: None,
                csv_has_header: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadArgs {
                path: args.input_path.clone(),
                limit: None,
                offset: None,
                csv_has_header: None,
            },
        }),
        FileType::Csv => Box::new(ReadCsvStep {
            path: args.input_path.clone(),
            has_header: args.input_headers,
        }),
        FileType::Orc => Box::new(ReadOrcStep {
            args: ReadArgs {
                path: args.input_path.clone(),
                limit: None,
                offset: None,
                csv_has_header: None,
            },
        }),
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for count"),
    };
    Ok(reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_parquet() {
        let args = CountArgs {
            input_path: "fixtures/table.parquet".to_string(),
            input: None,
            input_headers: None,
        };
        let result = count(args).await;
        assert!(result.is_ok(), "count failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_count_avro() {
        let args = CountArgs {
            input_path: "fixtures/userdata5.avro".to_string(),
            input: None,
            input_headers: None,
        };
        let result = count(args).await;
        assert!(result.is_ok(), "count failed: {:?}", result.err());
    }
}
