use arrow::record_batch::RecordBatch;
use arrow_json::ArrayWriter;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Pipeline step that writes record batches to a JSON file (single array of objects).
pub struct WriteJsonStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub args: WriteArgs,
}

pub struct WriteJsonResult {}

impl Step for WriteJsonStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = WriteJsonResult;

    fn execute(mut self) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        let reader = self.prev.get_record_batch_reader()?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
            .map_err(Error::ArrowError)?;
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        let mut writer = ArrayWriter::new(file);
        writer
            .write_batches(&batch_refs)
            .map_err(Error::ArrowError)?;
        writer.finish().map_err(Error::ArrowError)?;
        Ok(WriteJsonResult {})
    }
}
