//! [`Pipeline`] dispatch, blocking helper, and REPL-oriented read/write helpers.

use crate::FileType;
use crate::Result;
use crate::pipeline;
use crate::pipeline::Step;
use crate::pipeline::dataframe::DataFramePipeline;
use crate::pipeline::record_batch::RecordBatchPipeline;

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
    DataFrame(DataFramePipeline),
    RecordBatch(RecordBatchPipeline),
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
