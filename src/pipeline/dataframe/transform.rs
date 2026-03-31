//! Select, head/tail/sample, and finalize helpers on [`DataFrame`] / [`DataFrameSource`](super::source::DataFrameSource).
//! Stream a [`DataFrame`] as a [`RecordBatchReader`] via [`DataframeToRecordBatch`].

use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::DataFrame;
use datafusion::prelude::col;
use futures::StreamExt;

use super::source::DataFrameSource;
use crate::Error;
use crate::FileType;
use crate::pipeline::ColumnSpec;
use crate::pipeline::DisplaySlice;
use crate::pipeline::Producer;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;
use crate::pipeline::reservoir_sample_from_reader;
use crate::pipeline::sample_from_reader;
use crate::pipeline::tail_batches;

/// A `RecordBatchReader` that wraps a `DataFrameSource`, lazily streaming batches from the
/// underlying DataFrame via `execute_stream()`.
pub struct DataframeToRecordBatch {
    schema: Arc<arrow::datatypes::Schema>,
    stream: SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataframeToRecordBatch {
    /// Streams record batches from the [`DataFrame`] in `source`.
    pub async fn try_new(mut source: DataFrameSource) -> crate::Result<Self> {
        let df = *source.get().await?;
        let stream = df.execute_stream().await?;
        let schema = stream.schema();
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            schema,
            stream,
            handle,
        })
    }

    /// Collects all batches from the stream (errors are dropped).
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.filter_map(|r| r.ok()).collect()
    }
}

impl Iterator for DataframeToRecordBatch {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.clone();
        tokio::task::block_in_place(|| handle.block_on(self.stream.next()))
            .map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for DataframeToRecordBatch {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Keeps the first `n` rows (same semantics as [`DataFrame::limit`] with offset 0).
pub fn dataframe_apply_head(df: DataFrame, n: usize) -> crate::Result<DataFrame> {
    Ok(df.limit(0, Some(n))?)
}

/// Keeps the last `n` rows using metadata or full collection depending on `input_file_type`.
pub async fn dataframe_apply_tail(
    df: DataFrame,
    input_path: &str,
    input_file_type: FileType,
    tail_n: usize,
) -> crate::Result<DataFrame> {
    match input_file_type {
        FileType::Parquet => {
            let total_rows = crate::get_total_rows_result(input_path, FileType::Parquet)?;
            let number = tail_n.min(total_rows);
            let skip = total_rows.saturating_sub(number);
            Ok(df.limit(skip, Some(number))?)
        }
        FileType::Csv | FileType::Json | FileType::Avro | FileType::Orc => {
            let all = df.collect().await?;
            let batches = tail_batches(all, tail_n);
            Ok(SessionContext::new().read_batches(batches)?)
        }
        other => Err(Error::GenericError(format!(
            "DataFrame tail is not supported for input type: {other}"
        ))),
    }
}

/// Random sample of `n` rows using index sampling or reservoir sampling by input format.
pub async fn dataframe_apply_sample(
    df: DataFrame,
    input_path: &str,
    input_file_type: FileType,
    sample_n: usize,
) -> crate::Result<DataFrame> {
    match input_file_type {
        FileType::Parquet => {
            let total_rows = crate::get_total_rows_result(input_path, FileType::Parquet)?;
            let source = DataFrameSource::new(df);
            let batch_reader = DataframeToRecordBatch::try_new(source).await?;
            let batches = sample_from_reader(Box::new(batch_reader), total_rows, sample_n);
            Ok(SessionContext::new().read_batches(batches)?)
        }
        FileType::Avro | FileType::Csv | FileType::Json | FileType::Orc => {
            let source = DataFrameSource::new(df);
            let batch_reader = DataframeToRecordBatch::try_new(source).await?;
            let batches = reservoir_sample_from_reader(Box::new(batch_reader), sample_n);
            Ok(SessionContext::new().read_batches(batches)?)
        }
        other => Err(Error::GenericError(format!(
            "DataFrame sample is not supported for input type: {other}"
        ))),
    }
}

fn column_spec_in_group_keys(cs: &ColumnSpec, keys: &[ColumnSpec]) -> bool {
    keys.iter().any(|k| k == cs)
}

/// Applies `select()` projection, global aggregates, or grouped aggregates to a [`DataFrame`].
pub(super) fn apply_select_spec_to_dataframe(
    mut df: DataFrame,
    spec: &SelectSpec,
) -> crate::Result<DataFrame> {
    if spec.is_empty() {
        return Ok(df);
    }
    let schema = df.schema();
    let arrow_schema = schema.as_arrow();

    if spec.has_group_by() {
        let keys = spec.group_by.as_ref().expect("has_group_by implies Some");
        for key in keys {
            let mut found = false;
            for item in &spec.columns {
                if let SelectItem::Column(c) = item
                    && c == key
                {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(Error::GenericError(
                    "every group_by column must appear in select() as a plain column".to_string(),
                ));
            }
        }
        for item in &spec.columns {
            match item {
                SelectItem::Column(c) => {
                    if !column_spec_in_group_keys(c, keys) {
                        return Err(Error::GenericError(
                            "select with group_by: non-key columns must use sum(), not plain columns"
                                .to_string(),
                        ));
                    }
                }
                SelectItem::Sum(_) => {}
            }
        }

        let mut group_exprs = Vec::new();
        for key in keys {
            let name = key.resolve(arrow_schema)?;
            group_exprs.push(col(name.as_str()));
        }

        let mut aggs = Vec::new();
        for item in &spec.columns {
            if let SelectItem::Sum(cs) = item {
                let name = cs.resolve(arrow_schema)?;
                aggs.push(sum(col(name.as_str())));
            }
        }

        if aggs.is_empty() {
            eprintln!(
                "warning: group_by() with no aggregates in select(); showing distinct group keys only (behavior may change)"
            );
            let key_names: Vec<String> = keys
                .iter()
                .map(|k| k.resolve(arrow_schema))
                .collect::<crate::Result<Vec<_>>>()?;
            let col_refs: Vec<&str> = key_names.iter().map(String::as_str).collect();
            df = df.select_columns(&col_refs)?;
            df = df.distinct()?;
        } else {
            df = df.aggregate(group_exprs, aggs)?;
        }
        return Ok(df);
    }

    if spec.has_aggregates() {
        if !spec.is_aggregate_only() {
            return Err(Error::GenericError(
                "mixed column projections and aggregates in select require group_by(); \
                 put every group key in group_by() and list them as columns in select()"
                    .to_string(),
            ));
        }
        let mut aggs = Vec::new();
        for item in &spec.columns {
            if let SelectItem::Sum(cs) = item {
                let name = cs.resolve(arrow_schema)?;
                aggs.push(sum(col(name.as_str())));
            }
        }
        df = df.aggregate(vec![], aggs)?;
    } else {
        let resolved = spec.resolve_names(arrow_schema)?;
        let col_refs: Vec<&str> = resolved.iter().map(String::as_str).collect();
        df = df.select_columns(&col_refs)?;
    }
    Ok(df)
}

/// Applies optional column selection, SQL-style row limit, and display slice to a loaded [`DataFrame`].
///
/// Used by `LegacyDataFrameReader` and `dataframe_pipeline_prepare_source` so read → project →
/// cap/slice stays in one place.
pub(super) async fn finalize_dataframe_source(
    mut df: DataFrame,
    input_path: &str,
    input_file_type: FileType,
    select: Option<&SelectSpec>,
    limit: Option<usize>,
    slice: Option<DisplaySlice>,
) -> crate::Result<DataFrameSource> {
    if let Some(spec) = select
        && !spec.is_empty()
    {
        df = apply_select_spec_to_dataframe(df, spec)?;
    }
    if let Some(n) = limit {
        df = dataframe_apply_head(df, n)?;
    }
    if let Some(slice) = slice {
        df = match slice {
            DisplaySlice::Head(n) => dataframe_apply_head(df, n)?,
            DisplaySlice::Tail(tail_n) => {
                dataframe_apply_tail(df, input_path, input_file_type, tail_n).await?
            }
            DisplaySlice::Sample(n) => {
                dataframe_apply_sample(df, input_path, input_file_type, n).await?
            }
        };
    }
    Ok(DataFrameSource::new(df))
}
