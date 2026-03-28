//! DataFusion [`DataFrame`] pipeline: sources, writers, transforms, and CLI/REPL execution.

mod execute;
mod from_path;
mod source;
mod steps;
mod to_record_batch;
mod transform;
mod writer;

#[cfg(test)]
mod tests;

pub use execute::DataFramePipeline;
pub use execute::DataFrameSink;
pub use source::DataFrameSource;
pub use steps::DataframeHead;
pub use steps::DataframeSample;
pub use steps::DataframeSelect;
pub use steps::DataframeTail;
pub use to_record_batch::DataframeToRecordBatch;
pub use to_record_batch::DataframeToRecordBatchProducer;
pub use transform::dataframe_apply_head;
pub use transform::dataframe_apply_sample;
pub use transform::dataframe_apply_tail;
pub use writer::DataFrameWriter;
pub use writer::write_dataframe_pipeline_output;

pub use crate::pipeline::write::write_record_batches_from_reader;
