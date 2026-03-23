use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::get_total_rows_result;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::SelectSpec;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReader;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::build_reader;
use datu::pipeline::display::apply_select_and_display;
use datu::pipeline::read_to_batches;
use datu::pipeline::record_batch_filter::parse_select_step;
use datu::pipeline::reservoir_sample_from_reader;
use datu::pipeline::sample_from_reader;
use datu::resolve_file_type;
use eyre::Result;
use eyre::bail;

/// sample command implementation: print N random rows from an Avro, CSV, Parquet, or ORC file.
pub async fn sample(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    match input_file_type {
        FileType::Parquet => sample_seekable_format(args, FileType::Parquet).await,
        FileType::Avro => sample_avro(args).await,
        FileType::Csv => sample_csv(args).await,
        FileType::Orc => sample_seekable_format(args, FileType::Orc).await,
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for sample"),
    }
}

/// Samples N random rows from a seekable format file (Parquet or ORC) using metadata for total row count.
async fn sample_seekable_format(args: HeadsOrTails, file_type: FileType) -> Result<()> {
    let total_rows = get_total_rows_result(&args.input_path, file_type)?;
    let select = SelectSpec::from_cli_args(&args.select);

    let mut reader_step = build_reader(&args.input_path, file_type, None, None, None)?;
    if let Some(select_step) = parse_select_step(&select) {
        reader_step = select_step.execute(reader_step).await?;
    }
    let reader = reader_step.get()?;
    let sampled = sample_from_reader(reader, total_rows, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
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

/// Samples N random rows from an Avro file using reservoir sampling.
async fn sample_avro(args: HeadsOrTails) -> Result<()> {
    let select = SelectSpec::from_cli_args(&args.select);
    let mut reader_step = build_reader(&args.input_path, FileType::Avro, None, None, None)?;
    if let Some(select_step) = parse_select_step(&select) {
        reader_step = select_step.execute(reader_step).await?;
    }
    let reader = reader_step.get()?;
    let sampled = reservoir_sample_from_reader(reader, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
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

/// Samples N random rows from a CSV file using reservoir sampling.
async fn sample_csv(args: HeadsOrTails) -> Result<()> {
    let select = SelectSpec::from_cli_args(&args.select);
    let batches = read_to_batches(
        &args.input_path,
        FileType::Csv,
        &select,
        None,
        args.input_headers,
    )
    .await?;
    let reader: Box<dyn arrow::array::RecordBatchReader + 'static> =
        Box::new(VecRecordBatchReader::new(batches));
    let sampled = reservoir_sample_from_reader(reader, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
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
