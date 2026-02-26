use std::io::BufReader;

use arrow::array::RecordBatchReader;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::AvroWriter;

use crate::Error;
use crate::Result;
use crate::pipeline::LimitingRecordBatchReader;
use crate::pipeline::ReadArgs;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Pipeline step that reads an Avro file and produces a record batch reader.
pub struct ReadAvroStep {
    pub args: ReadArgs,
}

impl Source<dyn RecordBatchReader + 'static> for ReadAvroStep {
    fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
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
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

/// Result of successfully writing an Avro file.
pub struct WriteAvroResult {}

/// Write record batches from a reader to an Avro file.
pub fn write_record_batches(path: &str, reader: &mut dyn RecordBatchReader) -> Result<()> {
    let file = std::fs::File::create(path).map_err(Error::IoError)?;
    let schema = reader.schema();
    let mut writer = AvroWriter::new(file, (*schema).clone()).map_err(Error::ArrowError)?;
    for batch in reader {
        let batch = batch.map_err(Error::ArrowError)?;
        writer.write(&batch).map_err(Error::ArrowError)?;
    }
    writer.finish().map_err(Error::ArrowError)?;
    Ok(())
}

impl Step for WriteAvroStep {
    type Input = ();
    type Output = WriteAvroResult;

    fn execute(mut self, _input: Self::Input) -> Result<Self::Output> {
        let mut reader = self.source.get()?;
        write_record_batches(&self.args.path, &mut *reader)?;
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
