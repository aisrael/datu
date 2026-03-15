use std::io::BufReader;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::AvroWriter;
use async_trait::async_trait;

use crate::Error;
use crate::Result;
use crate::pipeline::ReadArgs;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;
use crate::pipeline::batch_write::BatchWriteSink;
use crate::pipeline::batch_write::write_record_batches_with_sink;

/// Returns true if the schema has any Int16 field (avro writer does not support Int16).
fn schema_has_int16(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| f.data_type() == &DataType::Int16)
}

/// Builds a schema suitable for the Avro writer: Int16 fields are replaced with Int32.
fn schema_for_avro_writer(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = if f.data_type() == &DataType::Int16 {
                DataType::Int32
            } else {
                f.data_type().clone()
            };
            Field::new(f.name(), dt, f.is_nullable())
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Casts Int16 columns in the batch to Int32 so the batch matches the Avro-compat schema.
fn cast_record_batch_for_avro(
    batch: &RecordBatch,
    compat_schema: &SchemaRef,
) -> arrow::error::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in compat_schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let target_type = field.data_type();
        let cast_col = if col.data_type() == &DataType::Int16 && target_type == &DataType::Int32 {
            compute::cast(col.as_ref(), target_type)?
        } else {
            col.clone()
        };
        columns.push(cast_col);
    }
    RecordBatch::try_new((*compat_schema).clone(), columns)
}

/// Wraps a record batch reader and exposes an Avro-compat schema (Int16 → Int32), casting each batch.
struct AvroCompatRecordBatchReader<'a> {
    reader: &'a mut dyn RecordBatchReader,
    compat_schema: SchemaRef,
}

impl RecordBatchReader for AvroCompatRecordBatchReader<'_> {
    fn schema(&self) -> SchemaRef {
        self.compat_schema.clone()
    }
}

impl Iterator for AvroCompatRecordBatchReader<'_> {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.reader.next()?;
        Some(batch.and_then(|b| cast_record_batch_for_avro(&b, &self.compat_schema)))
    }
}

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
/// Int16 columns are upcast to Int32 so the arrow-avro writer can write them.
pub fn write_record_batches(path: &str, reader: &mut dyn RecordBatchReader) -> Result<()> {
    let schema = reader.schema();
    if schema_has_int16(schema.as_ref()) {
        let compat_schema = schema_for_avro_writer(schema.as_ref());
        let mut wrapper = AvroCompatRecordBatchReader {
            reader,
            compat_schema,
        };
        write_record_batches_with_sink(path, &mut wrapper, AvroSink::new)
    } else {
        write_record_batches_with_sink(path, reader, AvroSink::new)
    }
}

struct AvroSink {
    writer: AvroWriter<std::fs::File>,
}

impl AvroSink {
    fn new(path: &str, schema: arrow::datatypes::SchemaRef) -> Result<Self> {
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        let writer = AvroWriter::new(file, (*schema).clone()).map_err(Error::ArrowError)?;
        Ok(Self { writer })
    }
}

impl BatchWriteSink for AvroSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(Error::ArrowError)
    }

    fn finish(mut self) -> Result<()> {
        self.writer.finish().map_err(Error::ArrowError)?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Step for WriteAvroStep {
    type Input = ();
    type Output = WriteAvroResult;

    async fn execute(mut self, _input: Self::Input) -> Result<Self::Output> {
        let mut reader = self.source.get()?;
        write_record_batches(&self.args.path, &mut *reader)?;
        Ok(WriteAvroResult {})
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow::array::Int16Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;

    use super::*;
    use crate::Error;
    use crate::pipeline::ReadArgs;
    use crate::pipeline::VecRecordBatchReader;

    #[test]
    fn test_write_avro_int16_upcast_to_int32() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int16, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int16Array::from(vec![1_i16, 2, 3]))],
        )
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("out.avro");
        let mut reader = VecRecordBatchReader::new(vec![batch]);
        write_record_batches(path.to_str().unwrap(), &mut reader).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let avro_reader = ReaderBuilder::new()
            .build(BufReader::new(file))
            .expect("written file should be valid Avro");
        let batches: Vec<_> = avro_reader.collect();
        assert_eq!(batches.len(), 1, "expected one batch");
        let batch = batches.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        let col = batch.column(0);
        assert_eq!(
            col.data_type(),
            &DataType::Int32,
            "Avro int is read as Int32"
        );
        let ints = col
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(ints.value(0), 1);
        assert_eq!(ints.value(1), 2);
        assert_eq!(ints.value(2), 3);
    }

    #[test]
    fn test_read_avro() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            limit: None,
            offset: None,
            csv_has_header: None,
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
            csv_has_header: None,
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
            csv_has_header: None,
        };
        let reader = read_avro(&args).expect("read_avro failed");
        let mut total_rows = 0usize;
        for batch_result in reader {
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
            csv_has_header: None,
        };
        let reader = read_avro(&args).expect("read_avro failed");
        let mut total_rows = 0usize;
        for batch_result in reader {
            let batch = batch_result.expect("Failed to read batch");
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5, "Expected 5 rows (skip 10, take 5)");
    }
}
