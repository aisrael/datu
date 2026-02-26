//! DataFusion DataFrame API for column selection.

use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::VecRecordBatchReaderSource;

/// Reads a Parquet file and selects columns using the DataFusion DataFrame API.
pub async fn read_parquet_select(
    path: &str,
    columns: &[String],
    limit: Option<usize>,
) -> crate::Result<RecordBatchReaderSource> {
    let ctx = SessionContext::new();
    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut df = ctx
        .read_parquet(path, ParquetReadOptions::default())
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    if !columns.is_empty() {
        df = df
            .select_columns(&col_refs)
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    }
    if let Some(n) = limit {
        df = df
            .limit(0, Some(n))
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    }
    let batches = df
        .collect()
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    Ok(Box::new(VecRecordBatchReaderSource::new(batches)))
}

/// Reads an Avro file and selects columns using the DataFusion DataFrame API.
pub async fn read_avro_select(
    path: &str,
    columns: &[String],
    limit: Option<usize>,
) -> crate::Result<RecordBatchReaderSource> {
    let ctx = SessionContext::new();
    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut df = ctx
        .read_avro(path, AvroReadOptions::default())
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    if !columns.is_empty() {
        df = df
            .select_columns(&col_refs)
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    }
    if let Some(n) = limit {
        df = df
            .limit(0, Some(n))
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    }
    let batches = df
        .collect()
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    Ok(Box::new(VecRecordBatchReaderSource::new(batches)))
}

/// Applies column selection to record batches using the DataFusion DataFrame API.
/// Returns the selected batches directly (for use when RecordBatchReaderSource is not needed).
pub async fn select_columns_to_batches(
    batches: Vec<RecordBatch>,
    columns: &[String],
) -> crate::Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let ctx = SessionContext::new();
    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let df = ctx
        .read_batches(batches)
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    let df = df
        .select_columns(&col_refs)
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    df.collect()
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))
}

/// Applies column selection to record batches using the DataFusion DataFrame API.
pub async fn select_columns_from_batches(
    batches: Vec<RecordBatch>,
    columns: &[String],
) -> crate::Result<RecordBatchReaderSource> {
    if batches.is_empty() {
        return Ok(Box::new(VecRecordBatchReaderSource::new(batches)));
    }
    let ctx = SessionContext::new();
    let col_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    let df = ctx
        .read_batches(batches)
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    let df = df
        .select_columns(&col_refs)
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    let result_batches = df
        .collect()
        .await
        .map_err(|e| crate::Error::GenericError(e.to_string()))?;
    Ok(Box::new(VecRecordBatchReaderSource::new(result_batches)))
}
