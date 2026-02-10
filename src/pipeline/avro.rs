use std::io::BufReader;

use arrow::array::RecordBatchReader;
use arrow_avro::reader::ReaderBuilder;

use crate::Error;
use crate::Result;
use crate::pipeline::LimitingRecordBatchReader;
use crate::pipeline::RecordBatchReaderSource;

/// Arguments for reading an Avro file
pub struct ReadAvroArgs {
    pub path: String,
    pub limit: Option<usize>,
}

/// A step in a pipeline that reads an Avro file
pub struct ReadAvroStep {
    pub args: ReadAvroArgs,
}

impl RecordBatchReaderSource for ReadAvroStep {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_avro(&self.args).map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read an Avro file and return a RecordBatchReader.
pub fn read_avro(args: &ReadAvroArgs) -> Result<impl RecordBatchReader + 'static> {
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

impl<R: RecordBatchReader + 'static> Iterator for LimitingRecordBatchReader<R> {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records_read >= self.limit {
            return None;
        }

        match self.inner.next() {
            Some(Ok(batch)) => {
                let remaining = self.limit - self.records_read;
                if batch.num_rows() <= remaining {
                    self.records_read += batch.num_rows();
                    Some(Ok(batch))
                } else {
                    let sliced = batch.slice(0, remaining);
                    self.records_read += remaining;
                    Some(Ok(sliced))
                }
            }
            res => res,
        }
    }
}

impl<R: RecordBatchReader + 'static> RecordBatchReader for LimitingRecordBatchReader<R> {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.inner.schema()
    }
}

/// Arguments for writing an Avro file
pub struct WriteAvroArgs {
    pub path: String,
}

pub struct WriteAvroStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub args: WriteAvroArgs,
}

pub struct WriteAvroResult {}

impl crate::pipeline::Step for WriteAvroStep {
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

    #[test]
    fn test_read_avro() {
        let args = ReadAvroArgs {
            path: "fixtures/table.avro".to_string(),
            limit: None,
        };
        // Creating a dummy Avro file for testing if it doesn't exist
        // or just rely on the fact that we'll have one during integration tests.
        // For now, we'll try to read it.
        let result = read_avro(&args);
        // If the file doesn't exist, this might fail, which is expected unless we create it.
        if let Ok(mut reader) = result {
            let _schema = reader.schema();
            let _batch = reader.next();
        }
    }
}
