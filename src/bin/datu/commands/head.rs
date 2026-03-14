use anyhow::Result;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::display::DisplayWriterStep;
use datu::pipeline::read_to_batches;

/// head command implementation: print the first N lines of an Avro, CSV, Parquet, or ORC file.
pub async fn head(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input_path.as_str().try_into()?;
    let batches = read_to_batches(
        &args.input_path,
        input_file_type,
        &args.select,
        Some(args.number),
        args.input_headers,
    )
    .await?;
    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(batches));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}
