//! DataFusion DataFrame API for column selection.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::VecRecordBatchReaderSource;

/// How to match a column name: exact (case-sensitive) or case-insensitive.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// Exact match (from string literal like "column").
    Exact(String),
    /// Case-insensitive match (from symbol like :column or bare identifier).
    CaseInsensitive(String),
}

impl ColumnSpec {
    /// Resolves this spec against a schema, returning the actual column name.
    pub fn resolve(&self, schema: &Schema) -> crate::Result<String> {
        match self {
            ColumnSpec::Exact(name) => schema
                .index_of(name)
                .map(|_| name.clone())
                .map_err(|e| crate::Error::GenericError(format!("Column '{name}' not found: {e}"))),
            ColumnSpec::CaseInsensitive(name) => schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(name))
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    crate::Error::GenericError(format!(
                        "Column '{name}' not found (case-insensitive match)"
                    ))
                }),
        }
    }
}

/// Resolves column specs to actual schema column names.
pub fn resolve_column_specs(schema: &Schema, specs: &[ColumnSpec]) -> crate::Result<Vec<String>> {
    specs.iter().map(|s| s.resolve(schema)).collect()
}

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
/// Resolves ColumnSpec against the schema: Exact uses case-sensitive match, CaseInsensitive uses
/// case-insensitive match.
pub async fn select_columns_to_batches(
    batches: Vec<RecordBatch>,
    specs: &[ColumnSpec],
) -> crate::Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let columns = resolve_column_specs(&schema, specs)?;
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;

    use super::*;

    fn schema_with_columns(names: &[&str]) -> Schema {
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect();
        Schema::new(fields)
    }

    #[test]
    fn test_resolve_exact_match() {
        let schema = schema_with_columns(&["one", "two", "three"]);
        let specs = vec![
            ColumnSpec::Exact("one".into()),
            ColumnSpec::Exact("three".into()),
        ];
        let resolved = resolve_column_specs(&schema, &specs).unwrap();
        assert_eq!(resolved, vec!["one", "three"]);
    }

    #[test]
    fn test_resolve_exact_no_match_wrong_case() {
        let schema = schema_with_columns(&["one", "two"]);
        let specs = vec![ColumnSpec::Exact("ONE".into())];
        let result = resolve_column_specs(&schema, &specs);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_case_insensitive_match() {
        let schema = schema_with_columns(&["one", "two", "Email"]);
        let specs = vec![
            ColumnSpec::CaseInsensitive("ONE".into()),
            ColumnSpec::CaseInsensitive("email".into()),
        ];
        let resolved = resolve_column_specs(&schema, &specs).unwrap();
        assert_eq!(resolved, vec!["one", "Email"]);
    }

    #[test]
    fn test_resolve_case_insensitive_no_match() {
        let schema = schema_with_columns(&["one", "two"]);
        let specs = vec![ColumnSpec::CaseInsensitive("missing".into())];
        let result = resolve_column_specs(&schema, &specs);
        assert!(result.is_err());
    }
}
