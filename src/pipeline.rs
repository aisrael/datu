//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod batch_write;
pub mod csv;
pub mod dataframe;
pub mod datasource;
pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod read;
pub mod record_batch_filter;
pub mod select;
pub mod write;
pub mod xlsx;
pub mod yaml;

use arrow::array::RecordBatchReader;
use async_trait::async_trait;
use futures::StreamExt;

use crate::FileType;
use crate::Result;
use crate::pipeline::dataframe::DataFrameReader;
use crate::pipeline::dataframe::DataFrameSource;
pub use crate::pipeline::read::ReadArgs;
pub use crate::pipeline::write::WriteArgs;
pub use crate::pipeline::write::WriteJsonArgs;
pub use crate::pipeline::write::WriteYamlArgs;

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
#[async_trait(?Send)]
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    async fn execute(self, input: Self::Input) -> Result<Self::Output>;
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
/// Uses DataFusion for Parquet, Avro, and CSV; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
    csv_has_header: Option<bool>,
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    let source = {
        let select = select.clone();
        DataFrameReader::new(input_path, input_file_type, select, limit, csv_has_header)
    }
    .execute(())
    .await?;
    let reader = DataFrameToBatchReader::try_new(source)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(reader.into_batches())
}

/// Takes the last `n` rows from a sequence of record batches.
pub fn tail_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    n: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let number = n.min(total_rows);
    let skip = total_rows.saturating_sub(number);

    let mut result = Vec::new();
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
        result.push(batch.slice(start_in_batch, take));
        rows_emitted += take;
    }

    result
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

    let source = crate::pipeline::dataframe::DataFrameSource::new(df);
    let writer_step = crate::pipeline::dataframe::DataFrameWriter::new(
        output_path,
        output_file_type,
        sparse,
        json_pretty,
    );
    writer_step.execute(source).await?;
    Ok(())
}
