use std::io::BufReader;

use arrow::array::RecordBatchReader;
use arrow_avro::reader::ReaderBuilder;

use crate::Error;
use crate::Result;
use crate::pipeline::LimitingRecordBatchReader;
use crate::pipeline::ReadArgs;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Pipeline step that reads an Avro file and produces a record batch reader.
pub struct ReadAvroStep {
    pub args: ReadArgs,
}

impl RecordBatchReaderSource for ReadAvroStep {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_avro(&self.args).map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read an Avro file and return a RecordBatchReader.
pub fn read_avro(args: &ReadArgs) -> Result<impl RecordBatchReader + 'static> {
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let reader = BufReader::new(file);
    let arrow_reader = ReaderBuilder::new()
        .build(reader)
        .map_err(Error::ArrowError)?;

    if let Some(limit) = args.limit {
        Ok(Box::new(LimitingRecordBatchReader {
            inner: arrow_reader,
            limit,
            records_read: 0,
        }) as Box<dyn RecordBatchReader + 'static>)
    } else {
        Ok(Box::new(arrow_reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Pipeline step that writes record batches to an Avro file.
pub struct WriteAvroStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub args: WriteArgs,
}

pub struct WriteAvroResult {}

impl Step for WriteAvroStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = WriteAvroResult;

    fn execute(mut self) -> Result<Self::Output> {
        use arrow_avro::writer::AvroWriter;

        let path = self.args.path.as_str();
        let file = std::fs::File::create(path).map_err(Error::IoError)?;

        let reader = self.prev.get_record_batch_reader()?;
        let schema = reader.schema();

        let mut writer = AvroWriter::new(file, (*schema).clone()).map_err(Error::ArrowError)?;

        for batch in reader {
            let batch = batch.map_err(Error::ArrowError)?;
            writer.write(&batch).map_err(Error::ArrowError)?;
        }

        writer.finish().map_err(Error::ArrowError)?;

        Ok(WriteAvroResult {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::pipeline::ReadArgs;

    #[test]
    fn test_read_avro() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            limit: None,
            offset: None,
        };
        let mut reader = read_avro(&args).expect("read_avro failed");
        let schema = reader.schema();
        assert!(!schema.fields().is_empty(), "Schema should have columns");
        let batch = reader
            .next()
            .expect("Expected at least one batch")
            .map_err(Error::ArrowError)
            .expect("Failed to read batch");
        assert!(batch.num_rows() > 0, "Expected at least one row");
    }
}
