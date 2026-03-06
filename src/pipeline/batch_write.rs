use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::Error;
use crate::Result;

/// Per-format sink adapter for writing record batches.
pub trait BatchWriteSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self) -> Result<()>;
}

/// Shared harness for batch-oriented file writers.
pub fn write_record_batches_with_sink<S, BuildSink>(
    path: &str,
    reader: &mut dyn RecordBatchReader,
    build_sink: BuildSink,
) -> Result<()>
where
    S: BatchWriteSink,
    BuildSink: FnOnce(&str, SchemaRef) -> Result<S>,
{
    let schema = reader.schema();
    let mut sink = build_sink(path, schema)?;

    for batch in reader {
        let batch = batch.map_err(Error::ArrowError)?;
        sink.write_batch(&batch)?;
    }

    sink.finish()
}
