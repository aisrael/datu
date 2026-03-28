use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::DataFrame;

use crate::FileType;
use crate::Result;
use crate::pipeline::DataFrameSource;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::dataframe::write_record_batches_from_reader;
use crate::pipeline::display;
use crate::pipeline::read::ReadResult;
use crate::pipeline::read::read_to_dataframe;
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
        let result = read_to_dataframe(&self.path, FileType::Json, None).await?;
        let ReadResult::DataFrame(source) = result else {
            unreachable!()
        };
        Ok(source)
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataframeJsonReader {
    async fn get(&mut self) -> Result<Box<DataFrame>> {
        let result = read_to_dataframe(&self.path, FileType::Json, None).await?;
        let ReadResult::DataFrame(mut source) = result else {
            unreachable!()
        };
        source.get().await
    }
}

/// Writes a [`DataFrame`] to JSON using DataFusion's native [`DataFrame::write_json`].
pub struct DataframeJsonWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeJsonWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        df.write_json(&self.args.path, DataFrameWriteOptions::default(), None)
            .await?;
        Ok(())
    }
}

/// Writes a [`DataFrame`] to JSON using the display layer ([`write_record_batches_from_reader`])
/// so `--json-pretty` and non-default sparse emission are supported.
pub struct DataframeJsonPrettyWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeJsonPrettyWriter {
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
        )?;
        Ok(())
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
        let mut reader = source.get().await?;
        display::write_record_batches_as_json_to_path(
            path,
            &mut *reader,
            self.args.sparse,
            self.args.pretty,
        )?;
        Ok(())
    }
}
