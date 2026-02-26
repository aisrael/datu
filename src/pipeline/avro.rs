use std::io::BufReader;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::AvroWriter;

use crate::Error;
use crate::Result;
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

/// Record batch reader that applies offset and limit to an underlying reader.
/// Arrow Avro does not support offset/limit at the builder level, so we wrap
/// the reader and slice batches as needed.
struct LimitOffsetRecordBatchReader {
    reader: Box<dyn RecordBatchReader + 'static>,
    offset_remaining: usize,
    limit_remaining: Option<usize>,
}

impl LimitOffsetRecordBatchReader {
    fn new(
        reader: Box<dyn RecordBatchReader + 'static>,
        offset: usize,
        limit: Option<usize>,
    ) -> Self {
        Self {
            reader,
            offset_remaining: offset,
            limit_remaining: limit,
        }
    }
}

impl RecordBatchReader for LimitOffsetRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.reader.schema()
    }
}

impl Iterator for LimitOffsetRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit_remaining == Some(0) {
            return None;
        }

        let batch = self.reader.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };

        let batch_rows = batch.num_rows();
        if batch_rows == 0 {
            return Some(Ok(batch));
        }

        // Apply offset: skip rows at the start
        let (start, take) = if self.offset_remaining > 0 {
            if batch_rows <= self.offset_remaining {
                self.offset_remaining -= batch_rows;
                return self.next();
            }
            let start = self.offset_remaining;
            self.offset_remaining = 0;
            (start, batch_rows - start)
        } else {
            (0, batch_rows)
        };

        // Apply limit: cap how many rows we return
        let take = match self.limit_remaining {
            Some(remaining) => {
                let take = take.min(remaining);
                self.limit_remaining = Some(remaining - take);
                take
            }
            None => take,
        };

        if take == 0 {
            return self.next();
        }

        let result = if start == 0 && take == batch_rows {
            batch
        } else {
            batch.slice(start, take)
        };

        Some(Ok(result))
    }
}

/// Read an Avro file and return a RecordBatchReader.
pub fn read_avro(args: &ReadArgs) -> Result<impl RecordBatchReader + 'static> {
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let reader = BufReader::new(file);
    let arrow_reader = ReaderBuilder::new()
        .build(reader)
        .map_err(Error::ArrowError)?;

    let base_reader = Box::new(arrow_reader) as Box<dyn RecordBatchReader + 'static>;

    if args.offset.is_some() || args.limit.is_some() {
        let offset = args.offset.unwrap_or(0);
        let limit = args.limit;
        Ok(Box::new(LimitOffsetRecordBatchReader::new(
            base_reader,
            offset,
            limit,
        )) as Box<dyn RecordBatchReader + 'static>)
    } else {
        Ok(base_reader)
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

    #[test]
    fn test_read_avro_with_limit() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            limit: Some(1),
            offset: None,
        };
        let mut reader = read_avro(&args).expect("read_avro failed");
        let batch = reader
            .next()
            .expect("Expected at least one batch")
            .map_err(Error::ArrowError)
            .expect("Failed to read batch");
        assert_eq!(batch.num_rows(), 1, "Expected only 1 row");
    }

    #[test]
    fn test_read_avro_with_offset() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            limit: None,
            offset: Some(1),
        };
        let mut reader = read_avro(&args).expect("read_avro failed");
        let mut total_rows = 0usize;
        while let Some(batch_result) = reader.next() {
            let batch = batch_result.expect("Failed to read batch");
            total_rows += batch.num_rows();
        }
        // userdata5.avro has 1000 rows; with offset 1 we expect 999
        assert_eq!(total_rows, 999, "Expected 999 rows after skipping 1");
    }

    #[test]
    fn test_read_avro_with_offset_and_limit() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            limit: Some(5),
            offset: Some(10),
        };
        let mut reader = read_avro(&args).expect("read_avro failed");
        let mut total_rows = 0usize;
        while let Some(batch_result) = reader.next() {
            let batch = batch_result.expect("Failed to read batch");
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5, "Expected 5 rows (skip 10, take 5)");
    }
}
