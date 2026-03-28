//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod csv;
pub mod dataframe;

pub use avro::DataframeAvroWriter;
pub use avro::RecordBatchAvroWriter;
pub use csv::DataframeCsvWriter;
pub use csv::RecordBatchCsvWriter;
pub use dataframe::DataFramePipeline;
pub use dataframe::DataFrameSink;
pub use dataframe::DataFrameSource;
pub use dataframe::DataframeHead;
pub use dataframe::DataframeSample;
pub use dataframe::DataframeSelect;
pub use dataframe::DataframeTail;
pub use dataframe::DataframeToRecordBatch;
pub use json::DataframeJsonPrettyWriter;
pub use json::DataframeJsonReader;
pub use json::DataframeJsonWriter;
pub use json::RecordBatchJsonWriter;
pub use orc::OrcRecordBatchReader;
pub use orc::RecordBatchOrcWriter;
pub use parquet::DataframeParquetWriter;
pub use parquet::RecordBatchParquetWriter;
pub use record_batch::RecordBatchHead;
pub use record_batch::RecordBatchPipeline;
pub use record_batch::RecordBatchSample;
pub use record_batch::RecordBatchSelect;
pub use record_batch::RecordBatchSink;
pub use record_batch::RecordBatchTail;

pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod read;
pub use read::DataframeFormatReader;

/// Type alias for [`DataframeFormatReader`] when used for Parquet input.
pub type DataframeParquetReader = DataframeFormatReader;
/// Type alias for [`DataframeFormatReader`] when used for Avro input.
pub type DataframeAvroReader = DataframeFormatReader;
/// Type alias for [`DataframeFormatReader`] when used for CSV input.
pub type DataframeCsvReader = DataframeFormatReader;

pub mod record_batch;
pub mod schema;
pub mod write;
pub mod xlsx;
pub mod yaml;

mod batch_readers;
mod builder;
mod run;
mod sampling;
mod spec;
mod step;

pub(crate) use batch_readers::ProgressRecordBatchReader;
pub(crate) use batch_readers::ProgressVecRecordBatchReader;
pub use batch_readers::VecRecordBatchReader;
pub use batch_readers::VecRecordBatchReaderSource;
pub use batch_readers::build_reader;
pub use batch_readers::count_rows;
pub use builder::PipelineBuilder;
pub use run::Pipeline;
pub(crate) use run::block_on_pipeline_future;
pub use run::write_batches;
pub use sampling::reservoir_sample_from_reader;
pub use sampling::sample_from_reader;
pub use sampling::tail_batches;
pub use spec::ColumnSpec;
pub(crate) use spec::DisplaySlice;
pub use spec::SelectSpec;
pub use step::Producer;
pub use step::RecordBatchReaderSource;
pub use step::Step;

#[cfg(test)]
mod tests;
