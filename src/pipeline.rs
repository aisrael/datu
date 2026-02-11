//! The `pipeline` module is the core of the dtfu crate.

pub mod avro;
pub mod csv;
pub mod display;
pub mod json;
pub mod parquet;
pub mod record_batch_filter;
pub mod xlsx;

use arrow::array::RecordBatchReader;

use crate::Result;

/// Arguments for writing a file (CSV, Avro, Parquet, JSON, XLSX).
pub struct WriteArgs {
    pub path: String,
}

/// Concrete operations that can be executed in a pipeline
pub enum Operation {
    ReadAvro(avro::ReadAvroStep),
    ReadParquet(parquet::ReadParquetStep),
    WriteAvro(WriteArgs),
    WriteParquet(WriteArgs),
    WriteCsv(WriteArgs),
    WriteJson(WriteArgs),
    WriteXlsx(WriteArgs),
}

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    fn execute(self) -> Result<Self::Output>;
}

/// A source of `RecordBatchReader`
pub trait RecordBatchReaderSource {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader>>;
}

/// A RecordBatchReader that limits the number of rows read.
pub struct LimitingRecordBatchReader<Inner: RecordBatchReader + 'static> {
    inner: Inner,
    limit: usize,
    records_read: usize,
}

/// A RecordBatchReader that yields batches from a Vec.
pub struct VecRecordBatchReader {
    batches: Vec<arrow::record_batch::RecordBatch>,
    index: usize,
}

impl Iterator for VecRecordBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

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
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()))
    }
}

/// A RecordBatchReaderSource that yields batches from a Vec.
pub struct VecRecordBatchReaderSource {
    batches: Option<Vec<arrow::record_batch::RecordBatch>>,
}

impl VecRecordBatchReaderSource {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self {
            batches: Some(batches),
        }
    }
}

impl RecordBatchReaderSource for VecRecordBatchReaderSource {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader>> {
        let batches = std::mem::take(&mut self.batches)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))?;
        Ok(Box::new(VecRecordBatchReader { batches, index: 0 }))
    }
}
