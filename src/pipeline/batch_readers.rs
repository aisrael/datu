//! Vec-backed readers, progress wrappers, and [`build_reader`] / [`count_rows`].

use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::get_total_rows_result;
use crate::pipeline::avro::RecordBatchAvroReader;
use crate::pipeline::csv::ReadCsvStepRecordBatch;
use crate::pipeline::orc::OrcRecordBatchReader;
use crate::pipeline::parquet::RecordBatchParquetReader;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::step::Producer;
use crate::pipeline::step::RecordBatchReaderSource;

/// A RecordBatchReader that yields batches from a Vec.
pub struct VecRecordBatchReader {
    batches: Vec<RecordBatch>,
    index: usize,
}

impl VecRecordBatchReader {
    /// Creates a reader that iterates `batches` in order.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches, index: 0 }
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.batches.len() {
            return None;
        }
        let batch = self.batches[self.index].clone();
        self.index += 1;
        Some(Ok(batch))
    }
}

impl RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    }
}

pub(crate) struct ProgressVecRecordBatchReader {
    pub(crate) inner: VecRecordBatchReader,
    pub(crate) progress: Option<ProgressBar>,
}

impl Iterator for ProgressVecRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if let Ok(ref batch) = item
            && let Some(pb) = &self.progress
        {
            pb.inc(batch.num_rows() as u64);
        }
        Some(item)
    }
}

impl RecordBatchReader for ProgressVecRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// Wraps any [`RecordBatchReader`] and increments [`ProgressBar`] by row count per batch.
/// Used for streaming readers (e.g. ORC) where [`ProgressVecRecordBatchReader`] does not apply.
pub(crate) struct ProgressRecordBatchReader {
    pub(crate) inner: Box<dyn RecordBatchReader + 'static>,
    pub(crate) progress: ProgressBar,
}

impl Iterator for ProgressRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if let Ok(ref batch) = item {
            self.progress.inc(batch.num_rows() as u64);
        }
        Some(item)
    }
}

impl RecordBatchReader for ProgressRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// A concrete implementation of Source<dyn RecordBatchReader + 'static> that yiedls a VecRecordBatchReader.
pub struct VecRecordBatchReaderSource {
    batches: Option<Vec<RecordBatch>>,
}

impl VecRecordBatchReaderSource {
    /// Wraps `batches` in a one-shot [`Producer`] of [`VecRecordBatchReader`].
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches: Some(batches),
        }
    }
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for VecRecordBatchReaderSource {
    async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        let batches = std::mem::take(&mut self.batches)
            .ok_or_else(|| Error::GenericError("Reader already taken".to_string()))?;
        Ok(Box::new(VecRecordBatchReader { batches, index: 0 }))
    }
}

/// Builds a format-specific `RecordBatchReaderSource` for [`ReadArgs::file_type`].
pub fn build_reader(args: &ReadArgs) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match args.file_type {
        FileType::Parquet => Box::new(RecordBatchParquetReader { args: args.clone() }),
        FileType::Avro => Box::new(RecordBatchAvroReader { args: args.clone() }),
        FileType::Csv => Box::new(ReadCsvStepRecordBatch { args: args.clone() }),
        FileType::Orc => Box::new(OrcRecordBatchReader { args: args.clone() }),
        _ => {
            return Err(Error::GenericError(format!(
                "Unsupported file type for reading: {}",
                args.file_type
            )));
        }
    };
    Ok(reader)
}

/// Counts rows in a file. Uses metadata for Parquet and ORC (no data read);
/// streams batches for Avro and CSV.
pub async fn count_rows(
    path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<usize> {
    if matches!(file_type, FileType::Parquet | FileType::Orc) {
        return get_total_rows_result(path, file_type);
    }
    let mut read_args = ReadArgs::new(path, file_type);
    read_args.csv_has_header = csv_has_header;
    let mut reader_step = build_reader(&read_args)?;
    let reader = reader_step.get().await?;
    let mut total = 0usize;
    for batch in reader {
        let batch = batch?;
        total += batch.num_rows();
    }
    Ok(total)
}
