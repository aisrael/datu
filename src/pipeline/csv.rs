use arrow::array::RecordBatchReader;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::WriteArgs;
use crate::pipeline::batch_write::BatchWriteSink;
use crate::pipeline::batch_write::write_record_batches_with_sink;

/// Pipeline step that reads a CSV file and produces a record batch reader.
/// Uses DataFusion for schema inference and type detection.
pub struct ReadCsvStep {
    pub path: String,
    pub has_header: Option<bool>,
    /// Maximum number of rows to read. None means read all.
    pub limit: Option<usize>,
}

impl Source<dyn RecordBatchReader + 'static> for ReadCsvStep {
    fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        let has_header = self.has_header.unwrap_or(true);
        let ctx = SessionContext::new();
        let df = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(ctx.read_csv(&self.path, CsvReadOptions::new().has_header(has_header)))
        })
        .map_err(|e| Error::GenericError(e.to_string()))?;

        let mut batches = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(df.collect())
        })
        .map_err(|e| Error::GenericError(e.to_string()))?;

        if let Some(limit) = self.limit {
            let mut result = Vec::new();
            let mut remaining = limit;
            for batch in batches {
                if remaining == 0 {
                    break;
                }
                let rows = batch.num_rows().min(remaining);
                result.push(batch.slice(0, rows));
                remaining -= rows;
            }
            batches = result;
        }

        Ok(Box::new(VecRecordBatchReader::new(batches)))
    }
}

/// Pipeline step that writes record batches to a CSV file.
pub struct WriteCsvStep {
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

/// Result of successfully writing a CSV file.
pub struct WriteCsvResult {}

/// Write record batches from a reader to a CSV file.
pub fn write_record_batches(path: &str, reader: &mut dyn RecordBatchReader) -> Result<()> {
    write_record_batches_with_sink(path, reader, CsvSink::new)
}

struct CsvSink {
    writer: arrow::csv::Writer<std::fs::File>,
}

impl CsvSink {
    fn new(path: &str, _schema: arrow::datatypes::SchemaRef) -> Result<Self> {
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        Ok(Self {
            writer: arrow::csv::Writer::new(file),
        })
    }
}

impl BatchWriteSink for CsvSink {
    fn write_batch(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(Error::ArrowError)
    }

    fn finish(self) -> Result<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
impl Step for WriteCsvStep {
    type Input = ();
    type Output = WriteCsvResult;

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let mut source = self.source;
        let mut reader = source.get()?;
        write_record_batches(self.args.path.as_str(), &mut *reader)?;
        Ok(WriteCsvResult {})
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;

    use super::*;
    use crate::pipeline::LegacyReadArgs;
    use crate::pipeline::Source;
    use crate::pipeline::parquet::read_parquet;

    struct TestRecordBatchReader {
        reader: Option<Box<dyn RecordBatchReader>>,
    }

    impl Source<dyn RecordBatchReader + 'static> for TestRecordBatchReader {
        fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
            std::mem::take(&mut self.reader)
                .ok_or(Error::GenericError("Reader already taken".to_string()))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_csv_writer() {
        let args = LegacyReadArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: None,
            offset: None,
            csv_has_header: None,
        };
        let reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.csv");
        let path = output_path
            .to_str()
            .expect("Failed to convert path to string")
            .to_string();

        let source: RecordBatchReaderSource = Box::new(TestRecordBatchReader {
            reader: Some(Box::new(reader)),
        });

        let args = WriteArgs { path };
        let writer = WriteCsvStep { args, source };
        let result = writer.execute(()).await;
        assert!(result.is_ok());
        assert!(output_path.exists());
    }
}
