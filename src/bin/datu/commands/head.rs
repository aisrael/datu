use anyhow::Result;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::display::DisplayWriterStep;

use super::convert;

/// head command implementation: print the first N lines of an Avro, Parquet, or ORC file.
pub async fn head(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    let batches = convert::read_to_batches(
        &args.input,
        input_file_type,
        &args.select,
        Some(args.number),
    )
    .await?;
    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(batches));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
    };
    display_step.execute(reader_step).map_err(Into::into)
}
