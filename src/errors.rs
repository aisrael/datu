use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("An error occurred: {0}")]
    GenericError(String),
    #[error("Unknown or unsupported file type: '{0}'")]
    UnknownFileType(String),
    #[error("Pipeline planning error: {0}")]
    PipelinePlanningError(String),
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
}
