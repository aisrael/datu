#[derive(thiserror::Error, Debug)]
pub enum WasmError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Unknown format: '{0}' (expected one of: parquet, avro, csv, json)")]
    UnknownFormat(String),
    #[error("input contains no record batches")]
    EmptyInput,
}
