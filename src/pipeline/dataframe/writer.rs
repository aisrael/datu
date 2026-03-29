//! [`DataFrameWriter`] and format dispatch for writing a [`DataFrame`] to disk.

use async_trait::async_trait;
use datafusion::prelude::DataFrame;

use super::source::DataFrameSource;
use crate::Error;
use crate::FileType;
use crate::errors::PipelineExecutionError;
use crate::pipeline::Producer;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::csv::DataframeCsvWriter;
use crate::pipeline::json::DataframeJsonPrettyWriter;
use crate::pipeline::json::DataframeJsonWriter;
use crate::pipeline::parquet;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::write_record_batches_from_reader;

/// A step that writes a DataFusion DataFrame to an output file.
pub struct DataFrameWriter {
    output_path: String,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
}

impl DataFrameWriter {
    /// Configures output path, format, and JSON/YAML emission options.
    pub fn new<S: Into<String>>(
        output_path: S,
        output_file_type: FileType,
        sparse: bool,
        json_pretty: bool,
    ) -> Self {
        Self {
            output_path: output_path.into(),
            output_file_type,
            sparse,
            json_pretty,
        }
    }
}

/// Writes a [`DataFrame`] to Parquet, CSV, or JSON by delegating to [`DataframeParquetWriter`],
/// [`DataframeCsvWriter`], and [`DataframeJsonWriter`] or [`DataframeJsonPrettyWriter`] when
/// pretty-printing or non-sparse JSON emission is requested.
pub async fn write_dataframe_pipeline_output(
    source: DataFrameSource,
    args: WriteArgs,
) -> crate::Result<()> {
    let input: Box<dyn Producer<DataFrame>> = Box::new(source);
    match args.file_type {
        FileType::Parquet => {
            parquet::DataframeParquetWriter { args }
                .execute(input)
                .await
        }
        FileType::Csv => DataframeCsvWriter { args }.execute(input).await,
        FileType::Json => {
            let needs_display_json = args.pretty.unwrap_or(false) || args.sparse == Some(false);
            if needs_display_json {
                DataframeJsonPrettyWriter { args }.execute(input).await
            } else {
                DataframeJsonWriter { args }.execute(input).await
            }
        }
        _ => Err(Error::PipelineExecutionError(
            PipelineExecutionError::UnsupportedOutputFileType(args.file_type),
        )),
    }
}

#[async_trait(?Send)]
impl Step for DataFrameWriter {
    type Input = DataFrameSource;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input.get().await?;

        let handle = tokio::runtime::Handle::current();
        let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))?;

        let mut reader = VecRecordBatchReader::new(batches);
        write_record_batches_from_reader(
            &mut reader,
            &self.output_path,
            self.output_file_type,
            self.sparse,
            self.json_pretty,
        )
    }
}
