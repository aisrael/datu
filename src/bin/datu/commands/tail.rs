use std::fs::File;

use anyhow::Result;
use anyhow::bail;
use datu::Error;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::ReadArgs;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::Step;
use datu::pipeline::VecRecordBatchReaderSource;
use datu::pipeline::avro::ReadAvroStep;
use datu::pipeline::display::DisplayWriterStep;
use datu::pipeline::orc::ReadOrcStep;
use datu::pipeline::parquet::ReadParquetStep;
use datu::pipeline::record_batch_filter::SelectColumnsStep;
use datu::utils::parse_select_columns;
use orc_rust::reader::metadata::read_metadata;
use parquet::file::metadata::ParquetMetaDataReader;

/// tail command implementation: print the last N lines of an Avro, Parquet, or ORC file.
pub fn tail(args: HeadsOrTails) -> Result<()> {
    let input_file_type: FileType = args.input.as_str().try_into()?;
    match input_file_type {
        FileType::Parquet => tail_parquet(args),
        FileType::Avro => tail_avro(args),
        FileType::Orc => tail_orc(args),
        _ => bail!("Only Parquet, Avro, and ORC are supported for tail"),
    }
}

/// Prints the last N lines of a Parquet file.
fn tail_parquet(args: HeadsOrTails) -> Result<()> {
    let meta_file = File::open(&args.input).map_err(Error::IoError)?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&meta_file)
        .map_err(Error::ParquetError)?;
    let total_rows = metadata.file_metadata().num_rows().max(0) as usize;
    let number = args.number.min(total_rows);
    let offset = total_rows.saturating_sub(number);

    let mut reader_step: RecordBatchReaderSource = Box::new(ReadParquetStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: Some(number),
            offset: Some(offset),
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        reader_step = Box::new(SelectColumnsStep {
            prev: reader_step,
            columns,
        });
    }
    let sparse = args.sparse;
    let display_step = DisplayWriterStep {
        prev: reader_step,
        output_format: args.output,
        sparse,
    };
    display_step.execute().map_err(Into::into)
}

/// Prints the last N rows from a generic record batch reader (used for Avro).
fn tail_from_reader(
    mut reader_step: RecordBatchReaderSource,
    number: usize,
    output: datu::cli::DisplayOutputFormat,
    sparse: bool,
) -> Result<()> {
    let reader = reader_step.get()?;
    let batches: Vec<arrow::record_batch::RecordBatch> = reader
        .map(|b| b.map_err(Error::ArrowError).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let number = number.min(total_rows);
    let skip = total_rows.saturating_sub(number);

    let mut tail_batches = Vec::new();
    let mut rows_emitted = 0usize;
    let mut rows_skipped = 0usize;
    for batch in batches {
        let batch_rows = batch.num_rows();
        if rows_skipped + batch_rows <= skip {
            rows_skipped += batch_rows;
            continue;
        }
        let start_in_batch = skip.saturating_sub(rows_skipped);
        rows_skipped += start_in_batch;
        let take = (number - rows_emitted).min(batch_rows - start_in_batch);
        if take == 0 {
            break;
        }
        let slice = batch.slice(start_in_batch, take);
        tail_batches.push(slice);
        rows_emitted += take;
    }

    let reader_step: RecordBatchReaderSource =
        Box::new(VecRecordBatchReaderSource::new(tail_batches));
    let display_step = DisplayWriterStep {
        prev: reader_step,
        output_format: output,
        sparse,
    };
    display_step.execute().map_err(Into::into)
}

/// Prints the last N lines of an Avro file.
fn tail_avro(args: HeadsOrTails) -> Result<()> {
    let mut reader_step: RecordBatchReaderSource = Box::new(ReadAvroStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: None,
            offset: None,
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        reader_step = Box::new(SelectColumnsStep {
            prev: reader_step,
            columns,
        });
    }
    let sparse = args.sparse;
    tail_from_reader(reader_step, args.number, args.output, sparse)
}

/// Prints the last N lines of an ORC file.
fn tail_orc(args: HeadsOrTails) -> Result<()> {
    let mut file = File::open(&args.input).map_err(Error::IoError)?;
    let metadata = read_metadata(&mut file).map_err(Error::OrcError)?;
    let total_rows = metadata.number_of_rows() as usize;
    let number = args.number.min(total_rows);
    let offset = total_rows.saturating_sub(number);

    let mut reader_step: RecordBatchReaderSource = Box::new(ReadOrcStep {
        args: ReadArgs {
            path: args.input.clone(),
            limit: Some(number),
            offset: Some(offset),
        },
    });
    if let Some(select) = &args.select {
        let columns = parse_select_columns(select);
        reader_step = Box::new(SelectColumnsStep {
            prev: reader_step,
            columns,
        });
    }
    let sparse = args.sparse;
    let display_step = DisplayWriterStep {
        prev: reader_step,
        output_format: args.output,
        sparse,
    };
    display_step.execute().map_err(Into::into)
}
