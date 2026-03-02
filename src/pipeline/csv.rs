use arrow::array::RecordBatchReader;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::WriteArgs;

/// Pipeline step that reads a CSV file and produces a record batch reader.
/// Uses DataFusion for schema inference and type detection.
pub struct ReadCsvStep {
    pub path: String,
    pub has_header: Option<bool>,
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

        let batches = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(df.collect())
        })
        .map_err(|e| Error::GenericError(e.to_string()))?;

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

impl Step for WriteCsvStep {
    type Input = ();
    type Output = WriteCsvResult;

    fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        let mut writer = arrow::csv::Writer::new(file);
        let mut source = self.source;
        let reader = source.get()?;
        for batch in reader {
            let batch = batch.map_err(Error::ArrowError)?;
            writer.write(&batch).map_err(Error::ArrowError)?;
        }
        Ok(WriteCsvResult {})
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;

    use super::*;
    use crate::pipeline::ReadArgs;
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

    #[test]
    fn test_csv_writer() {
        let args = ReadArgs {
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
        let result = writer.execute(());
        assert!(result.is_ok());
        assert!(output_path.exists());
    }
}
