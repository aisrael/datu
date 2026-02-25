use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

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
