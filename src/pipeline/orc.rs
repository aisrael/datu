use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::ArrowWriterBuilder;
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
        read_orc(&self.args).map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read an ORC file and return a RecordBatchReader.
///
/// When both offset and limit are specified, uses ORC row selection for efficient
/// seeking—only the requested rows are decoded, avoiding full file scans.
pub fn read_orc(args: &ReadArgs) -> Result<Box<dyn RecordBatchReader + 'static>> {
    expect_file_type(args, FileType::Orc)?;
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let builder = ArrowReaderBuilder::try_new(file).map_err(Error::OrcError)?;

    let arrow_reader = if let (Some(offset), Some(limit)) = (args.offset, args.limit) {
        let selection = vec![RowSelector::skip(offset), RowSelector::select(limit)].into();
        builder.with_row_selection(selection).build()
    } else {
        builder.build()
    };

    Ok(Box::new(arrow_reader))
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
