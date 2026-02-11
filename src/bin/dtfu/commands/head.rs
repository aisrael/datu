use anyhow::Result;
use anyhow::bail;
use dtfu::FileType;
use dtfu::cli::HeadsOrTails;
use dtfu::pipeline::RecordBatchReaderSource;
use dtfu::pipeline::Step;
use dtfu::pipeline::avro::ReadAvroArgs;
use dtfu::pipeline::avro::ReadAvroStep;
use dtfu::pipeline::display::DisplayWriterStep;
use dtfu::pipeline::parquet::ReadParquetArgs;
use dtfu::pipeline::parquet::ReadParquetStep;
use dtfu::pipeline::record_batch_filter::SelectColumnsStep;
use dtfu::utils::parse_select_columns;

/// head command implementation: print the first N lines of an Avro or Parquet file.
pub fn head(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let mut reader_step: Box<dyn RecordBatchReaderSource> =
        get_reader_step(input_file_type, &args)?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        reader_step = Box::new(SelectColumnsStep {
            prev: reader_step,
            columns,
        });
    }
    let display_step = DisplayWriterStep {
        prev: reader_step,
        output_format: args.output,
    };
    display_step.execute().map_err(Into::into)
}

fn get_reader_step(
    input_file_type: FileType,
    args: &HeadsOrTails,
) -> Result<Box<dyn RecordBatchReaderSource>> {
    let reader: Box<dyn RecordBatchReaderSource> = match input_file_type {
        FileType::Parquet => Box::new(ReadParquetStep {
            args: ReadParquetArgs {
                path: args.input.clone(),
                limit: Some(args.number),
                offset: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadAvroArgs {
                path: args.input.clone(),
                limit: Some(args.number),
            },
        }),
        _ => bail!("Only Parquet and Avro are supported for head"),
    };
    Ok(reader)
}
