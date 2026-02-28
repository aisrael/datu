//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod csv;
pub mod data_frame_reader;
pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod record_batch_filter;
pub mod select;
pub mod xlsx;
pub mod yaml;

use arrow::array::RecordBatchReader;
use futures::StreamExt;

use crate::FileType;
use crate::Result;
use crate::cli::convert::DataFrameSource;

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

/// A concrete implementation of Source<dyn RecordBatchReader + 'static> that yiedls a VecRecordBatchReader.
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

/// A `RecordBatchReader` that wraps a `DataFrameSource`, lazily streaming
/// batches from the underlying DataFrame via `execute_stream()`.
pub struct DataFrameToBatchReader {
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataFrameToBatchReader {
    pub async fn try_new(mut source: DataFrameSource) -> Result<Self> {
        let df = *source.get()?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
        let schema = stream.schema();
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            schema,
            stream,
            handle,
        })
    }

    pub fn into_batches(self) -> Vec<arrow::record_batch::RecordBatch> {
        self.filter_map(|r| r.ok()).collect()
    }
}

impl Iterator for DataFrameToBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.clone();
        tokio::task::block_in_place(|| handle.block_on(self.stream.next()))
            .map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for DataFrameToBatchReader {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Reads input into record batches for use by REPL and other callers that need RecordBatchReaderSource.
/// Uses DataFusion for Parquet and Avro; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    let source =
        data_frame_reader::read_dataframe(input_path, input_file_type, select.clone(), limit)
            .execute(())?;
    let reader = DataFrameToBatchReader::try_new(source)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(reader.into_batches())
}

/// Writes record batches to output file. Used by REPL.
pub async fn write_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> anyhow::Result<()> {
    let ctx = datafusion::execution::context::SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let source = crate::cli::convert::DataFrameSource::new(df);
    let writer_step = crate::cli::convert::DataFrameWriter::new(
        output_path,
        output_file_type,
        sparse,
        json_pretty,
    );
    writer_step.execute(source)?;
    Ok(())
}
