use std::fs::File;

use anyhow::Result;
use anyhow::bail;
use datu::Error;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::ReadArgs;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReader;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::avro::ReadAvroStep;
use datu::pipeline::display::DisplayWriterStep;
use datu::pipeline::orc::ReadOrcStep;
use datu::pipeline::parquet::ReadParquetStep;
use datu::pipeline::read_to_batches;
use datu::pipeline::record_batch_filter::SelectColumnsStep;
use datu::pipeline::reservoir_sample_from_reader;
use datu::pipeline::sample_from_reader;
use datu::utils::parse_select_columns;
use orc_rust::reader::metadata::read_metadata;
use parquet::file::metadata::ParquetMetaDataReader;

/// sample command implementation: print N random rows from an Avro, CSV, Parquet, or ORC file.
pub async fn sample(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    match input_file_type {
        FileType::Parquet => sample_parquet(args).await,
        FileType::Avro => sample_avro(args).await,
        FileType::Csv => sample_csv(args).await,
        FileType::Orc => sample_orc(args).await,
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for sample"),
    }
}

/// Samples N random rows from a Parquet file using metadata for total row count.
async fn sample_parquet(args: HeadsOrTails) -> Result<()> {
    let meta_file = File::open(&args.input).map_err(Error::IoError)?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&meta_file)
        .map_err(Error::ParquetError)?;
    let total_rows = metadata.file_metadata().num_rows().max(0) as usize;

    let mut reader_step: RecordBatchReaderSource = Box::new(ReadParquetStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: None,
            offset: None,
            csv_has_header: None,
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step).await?;
    }
    let reader = reader_step.get()?;
    let sampled = sample_from_reader(reader, total_rows, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}

/// Samples N random rows from an ORC file using metadata for total row count.
async fn sample_orc(args: HeadsOrTails) -> Result<()> {
    let mut file = File::open(&args.input).map_err(Error::IoError)?;
    let metadata = read_metadata(&mut file).map_err(Error::OrcError)?;
    let total_rows = metadata.number_of_rows() as usize;

    let mut reader_step: RecordBatchReaderSource = Box::new(ReadOrcStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: None,
            offset: None,
            csv_has_header: None,
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step).await?;
    }
    let reader = reader_step.get()?;
    let sampled = sample_from_reader(reader, total_rows, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}

/// Samples N random rows from an Avro file using reservoir sampling.
async fn sample_avro(args: HeadsOrTails) -> Result<()> {
    let mut reader_step: RecordBatchReaderSource = Box::new(ReadAvroStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: None,
            offset: None,
            csv_has_header: None,
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        let select_step = SelectColumnsStep { columns };
        reader_step = select_step.execute(reader_step).await?;
    }
    let reader = reader_step.get()?;
    let sampled = reservoir_sample_from_reader(reader, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}

/// Samples N random rows from a CSV file using reservoir sampling.
async fn sample_csv(args: HeadsOrTails) -> Result<()> {
    let batches = read_to_batches(
        &args.input,
        FileType::Csv,
        &args.select,
        None,
        args.input_headers,
    )
    .await?;
    let reader: Box<dyn arrow::array::RecordBatchReader + 'static> =
        Box::new(VecRecordBatchReader::new(batches));
    let sampled = reservoir_sample_from_reader(reader, args.number);

    let reader_step: RecordBatchReaderSource = Box::new(VecRecordBatchReaderSource::new(sampled));
    let display_step = DisplayWriterStep {
        output_format: args.output,
        sparse: args.sparse,
        headers: args.output_headers.unwrap_or(true),
    };
    display_step.execute(reader_step).await.map_err(Into::into)
}
