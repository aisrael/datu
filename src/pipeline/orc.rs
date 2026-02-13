use arrow::array::RecordBatchReader;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::ArrowWriterBuilder;
use orc_rust::row_selection::RowSelector;

use crate::Error;
use crate::Result;
use crate::pipeline::LimitingRecordBatchReader;
use crate::pipeline::ReadArgs;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Pipeline step that reads an ORC file and produces a record batch reader.
pub struct ReadOrcStep {
    pub args: ReadArgs,
}

impl Source<dyn RecordBatchReader + 'static> for ReadOrcStep {
    fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_orc(&self.args).map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Read an ORC file and return a RecordBatchReader.
///
/// When both offset and limit are specified, uses ORC row selection for efficient
/// seeking—only the requested rows are decoded, avoiding full file scans.
pub fn read_orc(args: &ReadArgs) -> Result<Box<dyn RecordBatchReader + 'static>> {
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
    let builder = ArrowReaderBuilder::try_new(file).map_err(Error::OrcError)?;

    let arrow_reader = if let (Some(offset), Some(limit)) = (args.offset, args.limit) {
        let selection = vec![RowSelector::skip(offset), RowSelector::select(limit)].into();
        builder.with_row_selection(selection).build()
    } else {
        builder.build()
    };

    if args.offset.is_none() {
        if let Some(limit) = args.limit {
            Ok(Box::new(LimitingRecordBatchReader {
                inner: arrow_reader,
                limit,
                records_read: 0,
            }))
        } else {
            Ok(Box::new(arrow_reader))
        }
    } else {
        Ok(Box::new(arrow_reader))
    }
}

/// Pipeline step that writes record batches to an ORC file.
pub struct WriteOrcStep {
    pub prev: RecordBatchReaderSource,
    pub args: WriteArgs,
}

pub struct WriteOrcResult {}

impl Step for WriteOrcStep {
    type Input = RecordBatchReaderSource;
    type Output = WriteOrcResult;

    fn execute(mut self) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let file = std::fs::File::create(path).map_err(Error::IoError)?;

        let reader = self.prev.get()?;
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
