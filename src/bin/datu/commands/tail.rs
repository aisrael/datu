use std::fs::File;

use anyhow::Result;
use anyhow::bail;
use datu::Error;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::build_reader;
use datu::pipeline::display::DisplayWriterStep;
use datu::pipeline::read_to_batches;
use datu::pipeline::record_batch_filter::SelectColumnsStep;
use datu::pipeline::tail_batches;
use datu::resolve_input_file_type;
use datu::utils::parse_select_columns;
use orc_rust::reader::metadata::read_metadata;
use parquet::file::metadata::ParquetMetaDataReader;

/// tail command implementation: print the last N lines of an Avro, CSV, Parquet, or ORC file.
pub async fn tail(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_input_file_type(args.input, &args.input_path)?;
    match input_file_type {
        FileType::Parquet => tail_parquet(args).await,
        FileType::Avro => tail_avro(args).await,
        FileType::Csv => tail_csv(args).await,
        FileType::Orc => tail_orc(args).await,
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for tail"),
    }
}

/// Prints the last N lines of a Parquet file.
async fn tail_parquet(args: HeadsOrTails) -> Result<()> {
    let meta_file = File::open(&args.input_path).map_err(Error::IoError)?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&meta_file)
        .map_err(Error::ParquetError)?;
    let total_rows = metadata.file_metadata().num_rows().max(0) as usize;
    let number = args.number.min(total_rows);
    let offset = total_rows.saturating_sub(number);

    let mut reader_step = build_reader(
        &args.input_path,
        FileType::Parquet,
        Some(number),
        Some(offset),
        None,
    )?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step).await?;
    }
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
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
    let display_step = DisplayWriterStep {
        output_format: output,
        sparse,
        headers,
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}

/// Prints the last N lines of a CSV file.
async fn tail_csv(args: HeadsOrTails) -> Result<()> {
    let batches = read_to_batches(
        &args.input_path,
        FileType::Csv,
        &args.select,
        None,
        args.input_headers,
    )
    .await?;
    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(batches));
    tail_from_reader(
        reader_step,
        args.number,
        args.output,
        args.sparse,
        args.output_headers.unwrap_or(true),
    )
    .await
}

/// Prints the last N lines of an Avro file.
async fn tail_avro(args: HeadsOrTails) -> Result<()> {
    let mut reader_step = build_reader(&args.input_path, FileType::Avro, None, None, None)?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
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

/// Prints the last N lines of an ORC file.
async fn tail_orc(args: HeadsOrTails) -> Result<()> {
    let mut file = File::open(&args.input_path).map_err(Error::IoError)?;
    let metadata = read_metadata(&mut file).map_err(Error::OrcError)?;
    let total_rows = metadata.number_of_rows() as usize;
    let number = args.number.min(total_rows);
    let offset = total_rows.saturating_sub(number);

    let mut reader_step = build_reader(
        &args.input_path,
        FileType::Orc,
        Some(number),
        Some(offset),
        None,
    )?;
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step).await?;
    }
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}
