use arrow::array::RecordBatchReader;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::ArrowWriterBuilder;

use crate::Error;
use crate::Result;
use crate::pipeline::LimitingRecordBatchReader;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Arguments for reading an ORC file.
pub struct ReadOrcArgs {
    pub path: String,
    pub limit: Option<usize>,
}

/// Pipeline step that reads an ORC file and produces a record batch reader.
pub struct ReadOrcStep {
    pub args: ReadOrcArgs,
}

impl RecordBatchReaderSource for ReadOrcStep {
    fn get_record_batch_reader(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_orc(&self.args).map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read an ORC file and return a RecordBatchReader.
pub fn read_orc(args: &ReadOrcArgs) -> Result<Box<dyn RecordBatchReader + 'static>> {
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let arrow_reader = ArrowReaderBuilder::try_new(file)
        .map_err(Error::OrcError)?
        .build();

    if let Some(limit) = args.limit {
        Ok(Box::new(LimitingRecordBatchReader {
            inner: arrow_reader,
            limit,
            records_read: 0,
        }))
    } else {
        Ok(Box::new(arrow_reader))
    }
}

/// Pipeline step that writes record batches to an ORC file.
pub struct WriteOrcStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub args: WriteArgs,
}

pub struct WriteOrcResult {}

impl Step for WriteOrcStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = WriteOrcResult;

    fn execute(mut self) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let file = std::fs::File::create(path).map_err(Error::IoError)?;

        let reader = self.prev.get_record_batch_reader()?;
        let schema = reader.schema();

        let mut writer = ArrowWriterBuilder::new(file, schema)
            .try_build()
            .map_err(Error::OrcError)?;

        for batch in reader {
            let batch = batch.map_err(Error::ArrowError)?;
            writer.write(&batch).map_err(Error::OrcError)?;
        }

        writer.close().map_err(Error::OrcError)?;

        Ok(WriteOrcResult {})
    }
}
