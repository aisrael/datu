use arrow::record_batch::RecordBatch;
use arrow_json::writer::JsonArray;
use arrow_json::writer::WriterBuilder;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteJsonArgs;

/// Pipeline step that writes record batches to a JSON file (single array of objects).
pub struct WriteJsonStep {
    pub args: WriteJsonArgs,
}

impl Step for WriteJsonStep {
    type Input = RecordBatchReaderSource;
    type Output = ();

    fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let reader = input.get()?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
            .map_err(Error::ArrowError)?;
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        let builder = WriterBuilder::new().with_explicit_nulls(!self.args.sparse);

        if self.args.pretty {
            let mut buf = Vec::new();
            let mut writer = builder.build::<_, JsonArray>(&mut buf);
            writer
                .write_batches(&batch_refs)
                .map_err(Error::ArrowError)?;
            writer.finish().map_err(Error::ArrowError)?;
            let value: serde_json::Value = serde_json::from_slice(&buf)
                .map_err(|e| Error::GenericError(format!("Invalid JSON: {e}")))?;
            let file = std::fs::File::create(path).map_err(Error::IoError)?;
            serde_json::to_writer_pretty(file, &value)
                .map_err(|e| Error::GenericError(format!("Failed to write JSON: {e}")))?;
        } else {
            let file = std::fs::File::create(path).map_err(Error::IoError)?;
            let mut writer = builder.build::<_, JsonArray>(file);
            writer
                .write_batches(&batch_refs)
                .map_err(Error::ArrowError)?;
            writer.finish().map_err(Error::ArrowError)?;
        }
        Ok(())
    }
}
