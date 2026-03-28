//! Column selection: ColumnSpec resolution (shared by CLI and REPL), in-memory projection
//! (Arrow), and DataFusion-based read helpers.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::DataFrame;
use datafusion::prelude::ParquetReadOptions;

use crate::Result;
use crate::pipeline::ColumnSpec;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::SelectSpec;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::dataframe::dataframe_apply_head;
use crate::pipeline::dataframe::dataframe_apply_select;

/// Resolves column specs to actual schema column names.
pub fn resolve_column_specs(schema: &Schema, specs: &[ColumnSpec]) -> Result<Vec<String>> {
    specs.iter().map(|s| s.resolve(schema)).collect()
}

/// Applies optional column projection, row cap, and materialization for a DataFusion [`DataFrame`].
async fn dataframe_select_limit_collect(
    df: DataFrame,
    spec: &SelectSpec,
    limit: Option<usize>,
) -> Result<RecordBatchReaderSource> {
    let mut df = dataframe_apply_select(df, Some(spec))?;
    if let Some(n) = limit {
        df = dataframe_apply_head(df, n)?;
    }
    let batches = df.collect().await?;
    Ok(Box::new(VecRecordBatchReaderSource::new(batches)))
}

/// Reads a Parquet file and selects columns using the DataFusion DataFrame API.
pub async fn read_parquet_select(
    path: &str,
    spec: &SelectSpec,
    limit: Option<usize>,
) -> Result<RecordBatchReaderSource> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(path, ParquetReadOptions::default())
        .await?;
    dataframe_select_limit_collect(df, spec, limit).await
}

/// Reads an Avro file and selects columns using the DataFusion DataFrame API.
pub async fn read_avro_select(
    path: &str,
    spec: &SelectSpec,
    limit: Option<usize>,
) -> Result<RecordBatchReaderSource> {
    let ctx = SessionContext::new();
    let df = ctx.read_avro(path, AvroReadOptions::default()).await?;
    dataframe_select_limit_collect(df, spec, limit).await
}

/// Applies column selection to record batches using the same resolution and projection
/// as the streaming [`RecordBatchSelect`](crate::pipeline::record_batch::RecordBatchSelect): resolve_column_specs then Arrow project by indices.
pub fn select_columns_to_batches(
    batches: Vec<RecordBatch>,
    specs: &[ColumnSpec],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || specs.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let column_names = resolve_column_specs(schema.as_ref(), specs)?;
    let indices: Vec<usize> = column_names
        .iter()
        .map(|col| Ok(schema.index_of(col)?))
        .collect::<Result<Vec<_>>>()?;
    batches
        .into_iter()
        .map(|batch| Ok(batch.project(&indices)?))
        .collect()
}

/// Applies column selection to record batches using the DataFusion DataFrame API.
pub async fn select_columns_from_batches(
    batches: Vec<RecordBatch>,
    spec: &SelectSpec,
) -> Result<RecordBatchReaderSource> {
    if batches.is_empty() {
        return Ok(Box::new(VecRecordBatchReaderSource::new(batches)) as RecordBatchReaderSource);
    }
    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches)?;
    dataframe_select_limit_collect(df, spec, None).await
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
