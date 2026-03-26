use arrow::record_batch::RecordBatch;
use arrow_json::writer::JsonArray;
use arrow_json::writer::WriterBuilder;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::SessionContext;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::pipeline::DataFrameSource;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::dataframe::write_record_batches_from_reader;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteJsonArgs;

/// Pipeline step that reads a JSON file and produces a DataFrame.
pub struct DataframeJsonReader {
    pub path: String,
}

#[async_trait(?Send)]
impl Step for DataframeJsonReader {
    type Input = ();
    type Output = DataFrameSource;

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_json(&self.path, NdJsonReadOptions::default())
            .await?;
        Ok(DataFrameSource::new(df))
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataframeJsonReader {
    async fn get(&mut self) -> Result<Box<DataFrame>> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_json(&self.path, NdJsonReadOptions::default())
            .await?;
        Ok(Box::new(df))
    }
}

/// Writes a [`DataFrame`] to a JSON file (NDJSON array semantics via [`write_record_batches_from_reader`]).
pub struct DataframeJsonWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeJsonWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        let batches = df.collect().await?;
        let mut reader = VecRecordBatchReader::new(batches);
        let sparse = self.args.sparse.unwrap_or(true);
        let json_pretty = self.args.pretty.unwrap_or(false);
        write_record_batches_from_reader(
            &mut reader,
            &self.args.path,
            FileType::Json,
            sparse,
            json_pretty,
        )
    }
}

/// Pipeline step that writes record batches to a JSON file (single array of objects).
pub struct RecordBatchJsonWriter {
    pub args: WriteJsonArgs,
    pub source: RecordBatchReaderSource,
}

#[async_trait(?Send)]
impl Step for RecordBatchJsonWriter {
    type Input = ();
    type Output = ();

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let mut source = self.source;
        let reader = source.get().await?;
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
