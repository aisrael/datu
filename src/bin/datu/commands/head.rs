use anyhow::Result;
use anyhow::bail;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::ReadArgs;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::avro::ReadAvroStep;
use datu::pipeline::display::DisplayWriterStep;
use datu::pipeline::orc::ReadOrcStep;
use datu::pipeline::parquet::ReadParquetStep;
use datu::pipeline::record_batch_filter::SelectColumnsStep;
use datu::utils::parse_select_columns;

/// head command implementation: print the first N lines of an Avro, Parquet, or ORC file.
pub fn head(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let mut reader_step: RecordBatchReaderSource = get_reader_step(input_file_type, &args)?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step)?;
    }
    let sparse = args.sparse;
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse,
    };
    display_step.execute(reader_step).map_err(Into::into)
}

/// Builds a record batch reader source for the given input file type and head args.
fn get_reader_step(
    input_file_type: FileType,
    args: &HeadsOrTails,
) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match input_file_type {
        FileType::Parquet => Box::new(ReadParquetStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: Some(args.number),
                offset: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: Some(args.number),
                offset: None,
            },
        }),
        FileType::Orc => Box::new(ReadOrcStep {
            args: ReadArgs {
                path: args.input.clone(),
                limit: Some(args.number),
                offset: None,
            },
        }),
        _ => bail!("Only Parquet, Avro, and ORC are supported for head"),
    };
    Ok(reader)
}
