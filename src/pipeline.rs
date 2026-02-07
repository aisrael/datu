//! The `pipeline` module is the core of the dtfu crate.

pub mod csv;
pub mod parquet;
pub mod record_batch_filter;

use arrow::array::RecordBatchReader;

use crate::Result;

/// Concrete operations that can be executed in a pipeline
pub enum Operation {
    ReadParquet(parquet::ReadParquetStep),
    WriteCsv(csv::WriteCsvArgs),
}

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    fn execute(&self, input: Self::Input) -> Result<Self::Output>;
}

/// A source of record batches
pub trait RecordBatchReaderSource {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader>>;
}
