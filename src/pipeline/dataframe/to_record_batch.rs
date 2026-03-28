//! Stream a [`DataFrame`] as a [`RecordBatchReader`] for record-batch writers.

use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use super::source::DataFrameSource;
use crate::Error;
use crate::pipeline::Producer;

/// A `RecordBatchReader` that wraps a `DataFrameSource`, lazily streaming batches from the
/// underlying DataFrame via `execute_stream()`.
pub struct DataframeToRecordBatch {
    schema: Arc<arrow::datatypes::Schema>,
    stream: SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataframeToRecordBatch {
    /// Streams record batches from the [`DataFrame`] in `source`.
    pub async fn try_new(mut source: DataFrameSource) -> crate::Result<Self> {
        let df = *source.get().await?;
        let stream = df.execute_stream().await?;
        let schema = stream.schema();
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            schema,
            stream,
            handle,
        })
    }

    /// Collects all batches from the stream (errors are dropped).
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.filter_map(|r| r.ok()).collect()
    }
}

impl Iterator for DataframeToRecordBatch {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.clone();
        tokio::task::block_in_place(|| handle.block_on(self.stream.next()))
            .map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for DataframeToRecordBatch {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Yields a [`DataframeToRecordBatch`] once from a [`DataFrameSource`] (for chaining to [`crate::pipeline::avro::RecordBatchAvroWriter`]).
pub struct DataframeToRecordBatchProducer {
    inner: Option<DataFrameSource>,
}

impl DataframeToRecordBatchProducer {
    /// Wraps `source` for a single [`DataframeToRecordBatch`] from [`Producer::get`].
    pub fn new(source: DataFrameSource) -> Self {
        Self {
            inner: Some(source),
        }
    }
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for DataframeToRecordBatchProducer {
    async fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        let source = self
            .inner
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame source already taken".to_string()))?;
        let reader = DataframeToRecordBatch::try_new(source).await?;
        Ok(Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}
