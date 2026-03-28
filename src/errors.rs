//! Error types for the datu crate.

use thiserror::Error;

use crate::FileType;

/// Errors that can occur during datu operations.
#[derive(Error, Debug)]
pub enum Error {
    #[error("An error occurred: {0}")]
    GenericError(String),
    #[error("Unknown or unsupported file type: '{0}'")]
    UnknownFileType(String),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error(transparent)]
    XlsxError(#[from] rust_xlsxwriter::XlsxError),
    #[error(transparent)]
    OrcError(#[from] orc_rust::error::OrcError),
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),
    #[error("Unsupported function call: {0}")]
    UnsupportedFunctionCall(String),
    #[error("{0} is not implemented in the REPL")]
    ReplNotImplemented(&'static str),
    #[error("Invalid REPL pipeline: {0}")]
    InvalidReplPipeline(String),
    #[error(transparent)]
    PipelinePlanningError(#[from] PipelinePlanningError),
    #[error(transparent)]
    PipelineExecutionError(#[from] PipelineExecutionError),
    #[error(transparent)]
    DatafusionError(#[from] datafusion::error::DataFusionError),
}

/// Errors produced while validating or constructing a pipeline plan.
#[derive(Error, Debug)]
pub enum PipelinePlanningError {
    #[error("select(...) cannot be empty")]
    SelectEmpty,
    #[error("Missing required stage: {0}")]
    MissingRequiredStage(String),
    #[error("Unsupported stage: {0}")]
    UnsupportedStage(String),
    #[error("Pipeline planner does not support input file type: {0}")]
    UnsupportedInputFileType(String),
    #[error("Pipeline planner does not support output file type: {0}")]
    UnsupportedOutputFileType(String),
    #[error("Column '{0}' not found (case-insensitive match)")]
    ColumnNotFound(String),
}

/// Errors produced while running a pipeline (wrong format, consumed state, etc.).
#[derive(Error, Debug)]
pub enum PipelineExecutionError {
    #[error("Datafusion-native pipeline does not support input file type: {0}")]
    UnsupportedInputFileType(FileType),
    #[error("Datafusion-native pipeline does not support output file type: {0}")]
    UnsupportedOutputFileType(FileType),
    #[error("DataFrame already taken")]
    DataFrameAlreadyTaken,
}
