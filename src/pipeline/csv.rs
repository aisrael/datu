use arrow::array::RecordBatchReader;
use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::DataFrame;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::read::expect_file_type;
use crate::pipeline::record_batch::BatchWriteSink;
use crate::pipeline::record_batch::apply_offset_limit;
use crate::pipeline::record_batch::write_record_batches_with_sink;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteResult;

/// Writes a [`DataFrame`] to CSV via DataFusion.
pub struct DataframeCsvWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeCsvWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        df.write_csv(&self.args.path, DataFrameWriteOptions::default(), None)
            .await?;
        Ok(())
    }
}

/// Pipeline step that reads a CSV file and produces a record batch reader.
/// Uses DataFusion for schema inference and type detection.
///
/// [`ReadArgs::file_type`] must be [`FileType::Csv`]. Applies [`ReadArgs::offset`] and
/// [`ReadArgs::limit`] with the same semantics as other record-batch readers (see [`ReadArgs`](crate::pipeline::read::ReadArgs)).
pub struct ReadCsvStepRecordBatch {
    pub args: ReadArgs,
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for ReadCsvStepRecordBatch {
    async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        expect_file_type(&self.args, FileType::Csv)?;
        let has_header = self.args.csv_has_header.unwrap_or(true);
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv(
                &self.args.path,
                CsvReadOptions::new().has_header(has_header),
            )
            .await?;

        let batches = df.collect().await?;

        let reader = Box::new(VecRecordBatchReader::new(batches));
        Ok(apply_offset_limit(
            reader,
            self.args.offset.unwrap_or(0),
            self.args.limit,
        ))
    }
}

/// Pipeline step that writes record batches to a CSV file.
pub struct RecordBatchCsvWriter {
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

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
impl Step for RecordBatchCsvWriter {
    type Input = ();
    type Output = WriteResult;

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let mut source = self.source;
        let mut reader = source.get().await?;
        write_record_batches(self.args.path.as_str(), &mut *reader)?;
        Ok(WriteResult)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;
    use async_trait::async_trait;

    use super::*;
    use crate::FileType;
    use crate::pipeline::Producer;
    use crate::pipeline::parquet::read_parquet;
    use crate::pipeline::read::ReadArgs;

    struct TestRecordBatchReader {
        reader: Option<Box<dyn RecordBatchReader>>,
    }

    #[async_trait(?Send)]
    impl Producer<dyn RecordBatchReader + 'static> for TestRecordBatchReader {
        async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
            std::mem::take(&mut self.reader)
                .ok_or(Error::GenericError("Reader already taken".to_string()))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_csv_writer() {
        let args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
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

        let args = WriteArgs {
            path,
            file_type: FileType::Csv,
            sparse: None,
            pretty: None,
        };
        let writer = RecordBatchCsvWriter { args, source };
        let result = writer.execute(()).await;
        assert!(result.is_ok());
        assert!(output_path.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_csv_record_batch_offset_and_limit() {
        let mut args = ReadArgs::new("fixtures/table.csv", FileType::Csv);
        args.offset = Some(1);
        args.limit = Some(1);
        let mut step = ReadCsvStepRecordBatch { args };
        let mut reader = step.get().await.expect("read csv batches");
        let total: usize = std::iter::from_fn(|| reader.next())
            .map(|b| b.expect("batch").num_rows())
            .sum();
        assert_eq!(total, 1, "Expected one row after offset 1 and limit 1");
    }
}
