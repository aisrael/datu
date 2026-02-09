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
    AvroError(#[from] Box<apache_avro::Error>),
}

// This lets us use the ? operator on AvroError
impl From<apache_avro::Error> for Error {
    fn from(err: apache_avro::Error) -> Self {
        Error::AvroError(Box::new(err))
    }
}
