use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::ArrowWriterBuilder;
use orc_rust::row_selection::RowSelection;
use orc_rust::row_selection::RowSelector;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::read::expect_file_type;
use crate::pipeline::record_batch::BatchWriteSink;
use crate::pipeline::record_batch::apply_offset_limit;
use crate::pipeline::record_batch::write_record_batches_with_sink;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteResult;

/// Pipeline step that reads an ORC file and produces a record batch reader.
pub struct OrcRecordBatchReader {
    pub args: ReadArgs,
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for OrcRecordBatchReader {
    async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_orc(&self.args)
    }
}

/// Read an ORC file and return a RecordBatchReader.
///
/// Uses ORC row selection when possible so slices avoid decoding skipped rows. If row metadata is
/// unavailable for offset-only reads, falls back to a streaming skip over a full decode.
pub fn read_orc(args: &ReadArgs) -> Result<Box<dyn RecordBatchReader + 'static>> {
    expect_file_type(args, FileType::Orc)?;
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let builder = ArrowReaderBuilder::try_new(file).map_err(Error::OrcError)?;

    let offset = args.offset.unwrap_or(0);
    let limit = args.limit;

    if offset == 0 && limit.is_none() {
        return Ok(Box::new(builder.build()));
    }

    if limit == Some(0) {
        let reader = builder.build();
        return Ok(apply_offset_limit(Box::new(reader), offset, Some(0)));
    }

    if let Some(lim) = limit.filter(|&l| l > 0) {
        if offset == 0 {
            let selection = vec![RowSelector::select(lim)].into();
            return Ok(Box::new(builder.with_row_selection(selection).build()));
        }
        let selection = vec![RowSelector::skip(offset), RowSelector::select(lim)].into();
        return Ok(Box::new(builder.with_row_selection(selection).build()));
    }

    if offset > 0 {
        match crate::get_total_rows_result(&args.path, FileType::Orc) {
            Ok(total) if offset >= total => {
                let selection = RowSelection::skip_all(total);
                return Ok(Box::new(builder.with_row_selection(selection).build()));
            }
            Ok(total) => {
                let selection =
                    RowSelection::from_consecutive_ranges(std::iter::once(offset..total), total);
                return Ok(Box::new(builder.with_row_selection(selection).build()));
            }
            Err(_) => {
                let reader = builder.build();
                return Ok(apply_offset_limit(Box::new(reader), offset, None));
            }
        }
    }

    unreachable!("read_orc: slice cases should cover all offset/limit combinations")
}

/// Read an entire ORC file into record batches (no row selection).
pub(crate) fn read_orc_all_batches(path: &str) -> Result<Vec<RecordBatch>> {
    let args = ReadArgs::new(path, FileType::Orc);
    let reader = read_orc(&args)?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// Pipeline step that writes record batches to an ORC file.
pub struct RecordBatchOrcWriter {
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

/// Write record batches from a reader to an ORC file.
pub fn write_record_batches(path: &str, reader: &mut dyn RecordBatchReader) -> Result<()> {
    write_record_batches_with_sink(path, reader, OrcSink::new)
}

struct OrcSink {
    writer: orc_rust::arrow_writer::ArrowWriter<std::fs::File>,
}

impl OrcSink {
    fn new(path: &str, schema: arrow::datatypes::SchemaRef) -> Result<Self> {
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        let writer = ArrowWriterBuilder::new(file, schema)
            .try_build()
            .map_err(Error::OrcError)?;
        Ok(Self { writer })
    }
}

impl BatchWriteSink for OrcSink {
    fn write_batch(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(Error::OrcError)
    }

    fn finish(self) -> Result<()> {
        self.writer.close().map_err(Error::OrcError)?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Step for RecordBatchOrcWriter {
    type Input = ();
    type Output = WriteResult;

    async fn execute(mut self, _input: Self::Input) -> Result<Self::Output> {
        let mut reader = self.source.get().await?;
        write_record_batches(&self.args.path, &mut *reader)?;
        Ok(WriteResult)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatchReader;

    use super::*;
    use crate::pipeline::read::ReadArgs;

    fn row_count(reader: Box<dyn RecordBatchReader + 'static>) -> usize {
        reader.map(|b| b.expect("batch").num_rows()).sum()
    }

    #[test]
    fn test_read_orc_limit_only() {
        let mut args = ReadArgs::new("fixtures/userdata.orc", FileType::Orc);
        args.limit = Some(1);
        let reader = read_orc(&args).expect("read_orc");
        assert_eq!(row_count(reader), 1);
    }

    #[test]
    fn test_read_orc_offset_only() {
        let total = crate::get_total_rows_result("fixtures/userdata.orc", FileType::Orc).unwrap();
        let mut args = ReadArgs::new("fixtures/userdata.orc", FileType::Orc);
        args.offset = Some(1);
        let reader = read_orc(&args).expect("read_orc");
        assert_eq!(row_count(reader), total.saturating_sub(1));
    }

    #[test]
    fn test_read_orc_offset_past_eof() {
        let total = crate::get_total_rows_result("fixtures/userdata.orc", FileType::Orc).unwrap();
        let mut args = ReadArgs::new("fixtures/userdata.orc", FileType::Orc);
        args.offset = Some(total.saturating_add(10));
        let reader = read_orc(&args).expect("read_orc");
        assert_eq!(row_count(reader), 0);
    }
}
