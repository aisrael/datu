//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod csv;
pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod record_batch_filter;
pub mod select;
pub mod xlsx;
pub mod yaml;

use arrow::array::RecordBatchReader;

use crate::Result;

/// Arguments for reading a file (Avro, Parquet, ORC).
pub struct ReadArgs {
    pub path: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Arguments for writing a file (CSV, Avro, Parquet, ORC, XLSX).
pub struct WriteArgs {
    pub path: String,
}

/// Arguments for writing a JSON file.
pub struct WriteJsonArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
    /// When true, format output with indentation and newlines.
    pub pretty: bool,
}

/// Arguments for writing a YAML file.
pub struct WriteYamlArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
}

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    fn execute(self, input: Self::Input) -> Result<Self::Output>;
}

/// A source that yields a value of type `T`.
pub trait Source<T: ?Sized> {
    /// Produces the next value from this source; consumes the source on first call.
    fn get(&mut self) -> Result<Box<T>>;
}

/// Type alias for a boxed source of `RecordBatchReader`.
pub type RecordBatchReaderSource = Box<dyn Source<dyn RecordBatchReader + 'static>>;

/// A RecordBatchReader that limits the number of rows read.
pub struct LimitingRecordBatchReader<Inner: RecordBatchReader + 'static> {
    inner: Inner,
    limit: usize,
    records_read: usize,
}

impl<Inner: RecordBatchReader + 'static> Iterator for LimitingRecordBatchReader<Inner> {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records_read >= self.limit {
            return None;
        }

        match self.inner.next() {
            Some(Ok(batch)) => {
                let remaining = self.limit - self.records_read;
                if batch.num_rows() <= remaining {
                    self.records_read += batch.num_rows();
                    Some(Ok(batch))
                } else {
                    let sliced = batch.slice(0, remaining);
                    self.records_read += remaining;
                    Some(Ok(sliced))
                }
            }
            res => res,
        }
    }
}

impl<Inner: RecordBatchReader + 'static> RecordBatchReader for LimitingRecordBatchReader<Inner> {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.inner.schema()
    }
}

/// A RecordBatchReader that yields batches from a Vec.
pub struct VecRecordBatchReader {
    batches: Vec<arrow::record_batch::RecordBatch>,
    index: usize,
}

impl VecRecordBatchReader {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self { batches, index: 0 }
    }
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

impl Source<dyn RecordBatchReader + 'static> for VecRecordBatchReaderSource {
    fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        let batches = std::mem::take(&mut self.batches)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))?;
        Ok(Box::new(VecRecordBatchReader { batches, index: 0 }))
    }
}
