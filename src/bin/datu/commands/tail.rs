use datu::Error;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::get_total_rows_result;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::SelectSpec;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::build_reader;
use datu::pipeline::display::apply_select_and_display;
use datu::pipeline::read_to_batches;
use datu::pipeline::record_batch_filter::parse_select_step;
use datu::pipeline::tail_batches;
use datu::resolve_file_type;
use eyre::Result;
use eyre::bail;

/// tail command implementation: print the last N lines of an Avro, CSV, Parquet, or ORC file.
pub async fn tail(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    match input_file_type {
        FileType::Parquet => tail_seekable_format(args, FileType::Parquet).await,
        FileType::Avro => tail_avro(args).await,
        FileType::Csv => tail_csv(args).await,
        FileType::Orc => tail_seekable_format(args, FileType::Orc).await,
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for tail"),
    }
}

/// Prints the last N lines of a seekable format file (Parquet or ORC) using metadata and offset.
async fn tail_seekable_format(args: HeadsOrTails, file_type: FileType) -> Result<()> {
    let total_rows = get_total_rows_result(&args.input_path, file_type)?;
    let number = args.number.min(total_rows);
    let offset = total_rows.saturating_sub(number);
    let select = SelectSpec::from_cli_args(&args.select);

    let reader_step = build_reader(
        &args.input_path,
        file_type,
        Some(number),
        Some(offset),
        None,
    )?;
    apply_select_and_display(
        reader_step,
        parse_select_step(&select),
        args.output,
        args.sparse,
        args.output_headers.unwrap_or(true),
    )
    .await
    .map_err(Into::into)
}

/// Prints the last N rows from a generic record batch reader (used for Avro).
async fn tail_from_reader(
    mut reader_step: RecordBatchReaderSource,
    number: usize,
    output: datu::cli::DisplayOutputFormat,
    sparse: bool,
    headers: bool,
) -> Result<()> {
    let reader = reader_step.get()?;
    let batches: Vec<arrow::record_batch::RecordBatch> = reader
        .map(|b| b.map_err(Error::ArrowError).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    let tail_batches = tail_batches(batches, number);

    let reader_step: RecordBatchReaderSource =
        Box::new(VecRecordBatchReaderSource::new(tail_batches));
    apply_select_and_display(reader_step, None, output, sparse, headers)
        .await
        .map_err(Into::into)
}

/// Prints the last N lines of a CSV file.
async fn tail_csv(args: HeadsOrTails) -> Result<()> {
    let select = SelectSpec::from_cli_args(&args.select);
    let batches = read_to_batches(
        &args.input_path,
        FileType::Csv,
        &select,
        None,
        args.input_headers,
    )
    .await?;
    let tailed = tail_batches(batches, args.number);
    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(tailed));
    apply_select_and_display(
        reader_step,
        None,
        args.output,
        args.sparse,
        args.output_headers.unwrap_or(true),
    )
    .await
    .map_err(Into::into)
}

/// Prints the last N lines of an Avro file.
async fn tail_avro(args: HeadsOrTails) -> Result<()> {
    let select = SelectSpec::from_cli_args(&args.select);
    let mut reader_step = build_reader(&args.input_path, FileType::Avro, None, None, None)?;
    if let Some(select_step) = parse_select_step(&select) {
        reader_step = select_step.execute(reader_step).await?;
    }
    tail_from_reader(
        reader_step,
        args.number,
        args.output,
        args.sparse,
        args.output_headers.unwrap_or(true),
    )
    .await
}
