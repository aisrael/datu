//! [`Pipeline`] dispatch, blocking helper, and REPL-oriented read/write helpers.

use crate::FileType;
use crate::Result;
use crate::pipeline;
use crate::pipeline::Step;
use crate::pipeline::dataframe::DataFramePipeline;
use crate::pipeline::dataframe::LegacyDataFrameReader;
use crate::pipeline::record_batch::RecordBatchPipeline;
use crate::pipeline::spec::SelectSpec;

/// Runs a pipeline future on the current Tokio runtime when called from within a runtime
/// worker; otherwise builds a current-thread runtime. Used by synchronous `execute` methods
/// that wrap async bodies.
pub(crate) fn block_on_pipeline_future<T, F>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(fut)
    }
}

/// Result of [`crate::pipeline::PipelineBuilder::build`]: DataFusion path vs ORC record-batch path (see `PIPELINE.mermaid`).
pub enum Pipeline {
    DataFrame(Box<DataFramePipeline>),
    RecordBatch(Box<RecordBatchPipeline>),
}

impl Pipeline {
    /// Runs read → select → head/tail/sample → sink (file or stdout).
    pub fn execute(&mut self) -> Result<()> {
        match self {
            Pipeline::DataFrame(p) => p.execute(),
            Pipeline::RecordBatch(p) => p.execute(),
        }
    }
}

/// Reads input into record batches for use by REPL and other callers that need RecordBatchReaderSource.
/// Uses DataFusion for Parquet, Avro, and CSV; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<SelectSpec>,
    limit: Option<usize>,
    csv_has_header: Option<bool>,
) -> eyre::Result<Vec<arrow::record_batch::RecordBatch>> {
    let source = {
        let select = select.clone();
        LegacyDataFrameReader::new(input_path, input_file_type, select, limit, csv_has_header)
    }
    .execute(())
    .await?;
    let reader = pipeline::dataframe::DataframeToRecordBatch::try_new(source)
        .await
        .map_err(|e| eyre::eyre!("{e}"))?;
    Ok(reader.into_batches())
}

/// Writes record batches to output file. Used by REPL.
pub async fn write_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> eyre::Result<()> {
    let ctx = datafusion::execution::context::SessionContext::new();
    let df = ctx.read_batches(batches).map_err(|e| eyre::eyre!("{e}"))?;

    let source = pipeline::dataframe::DataFrameSource::new(df);
    let writer_step = pipeline::dataframe::DataFrameWriter::new(
        output_path,
        output_file_type,
        sparse,
        json_pretty,
    );
    writer_step.execute(source).await?;
    Ok(())
}
