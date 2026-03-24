use arrow::array::RecordBatchReader;
use async_trait::async_trait;

use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
use crate::pipeline::ColumnSpec;
use crate::pipeline::DisplayPipeline;
use crate::pipeline::DisplaySlice;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::SelectSpec;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::build_reader;
use crate::pipeline::display::apply_select_and_display;
use crate::pipeline::sample_from_reader;
use crate::pipeline::select::resolve_column_specs;

/// A Source that wraps a single RecordBatchReader and yields it on get().
struct RecordBatchReaderHolder {
    reader: Option<Box<dyn RecordBatchReader + 'static>>,
}

impl Source<dyn RecordBatchReader + 'static> for RecordBatchReaderHolder {
    fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        std::mem::take(&mut self.reader)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))
    }
}

/// Pipeline step that filters record batches to only the specified columns.
pub struct SelectColumnsStep {
    pub columns: Vec<ColumnSpec>,
}

#[async_trait(?Send)]
impl Step for SelectColumnsStep {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let reader = input.get()?;
        let schema = reader.schema();
        let column_names = resolve_column_specs(&schema, &self.columns)?;
        let indices: Vec<usize> = column_names
            .iter()
            .map(|col| {
                schema.index_of(col).map_err(|e| {
                    crate::Error::GenericError(format!("Column '{col}' not found: {e}"))
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let projected_schema = reader.schema().project(&indices)?;
        let projected_reader = SelectColumnRecordBatchReader {
            reader,
            schema: std::sync::Arc::new(projected_schema),
            indices,
        };
        Ok(Box::new(RecordBatchReaderHolder {
            reader: Some(Box::new(projected_reader)),
        }))
    }
}

/// Record batch reader that projects only the selected column indices.
pub struct SelectColumnRecordBatchReader {
    reader: Box<dyn RecordBatchReader>,
    schema: arrow::datatypes::SchemaRef,
    indices: Vec<usize>,
}

impl RecordBatchReader for SelectColumnRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for SelectColumnRecordBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader
            .next()
            .map(|batch| batch.and_then(|b| b.project(&self.indices)))
    }
}

/// Builds a [`SelectColumnsStep`] from a resolved [`SelectSpec`].
/// Returns None when no selection is requested.
pub fn parse_select_step(select: &Option<SelectSpec>) -> Option<SelectColumnsStep> {
    let spec = select.as_ref()?;
    if spec.is_empty() {
        return None;
    }
    Some(SelectColumnsStep {
        columns: spec.columns.clone(),
    })
}

/// ORC display via [`crate::pipeline::build_reader`] (DataFusion does not read ORC natively).
pub struct RecordBatchDisplayPipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    pub(crate) slice: DisplaySlice,
    pub(crate) csv_has_header: Option<bool>,
    pub(crate) output_format: DisplayOutputFormat,
    pub(crate) sparse: bool,
    pub(crate) csv_stdout_headers: bool,
}

impl DisplayPipeline for RecordBatchDisplayPipeline {
    fn execute(&mut self) -> crate::Result<()> {
        if self.input_file_type != FileType::Orc {
            return Err(Error::GenericError(format!(
                "RecordBatchDisplayPipeline only supports ORC input, got {}",
                self.input_file_type
            )));
        }

        let input_path = self.input_path.clone();
        let select = self.select.clone();
        let slice = self.slice;
        let csv_has_header = self.csv_has_header;
        let output_format = self.output_format;
        let sparse = self.sparse;
        let csv_stdout_headers = self.csv_stdout_headers;

        let fut = async move {
            match slice {
                DisplaySlice::Head(head_n) => {
                    let reader = build_reader(
                        &input_path,
                        FileType::Orc,
                        Some(head_n),
                        Some(0),
                        csv_has_header,
                    )?;
                    apply_select_and_display(
                        reader,
                        parse_select_step(&select),
                        output_format,
                        sparse,
                        csv_stdout_headers,
                    )
                    .await
                }
                DisplaySlice::Tail(tail_n) => {
                    let total_rows = crate::get_total_rows_result(&input_path, FileType::Orc)?;
                    let number = tail_n.min(total_rows);
                    let offset = total_rows.saturating_sub(number);
                    let reader =
                        build_reader(&input_path, FileType::Orc, Some(number), Some(offset), None)?;
                    apply_select_and_display(
                        reader,
                        parse_select_step(&select),
                        output_format,
                        sparse,
                        csv_stdout_headers,
                    )
                    .await
                }
                DisplaySlice::Sample(n) => {
                    let total_rows = crate::get_total_rows_result(&input_path, FileType::Orc)?;
                    let mut reader_step =
                        build_reader(&input_path, FileType::Orc, None, None, csv_has_header)?;
                    if let Some(select_step) = parse_select_step(&select) {
                        reader_step = select_step.execute(reader_step).await?;
                    }
                    let reader = reader_step.get()?;
                    let sampled = sample_from_reader(reader, total_rows, n);
                    let out: RecordBatchReaderSource =
                        Box::new(VecRecordBatchReaderSource::new(sampled));
                    apply_select_and_display(out, None, output_format, sparse, csv_stdout_headers)
                        .await
                }
            }
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| Error::GenericError(e.to_string()))?;
            rt.block_on(fut)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ColumnSpec;
    use crate::pipeline::ReadArgs;
    use crate::pipeline::RecordBatchReaderSource;
    use crate::pipeline::SelectSpec;
    use crate::pipeline::parquet::ReadParquetStep;

    #[test]
    fn test_parse_select_step_none() {
        assert!(parse_select_step(&None).is_none());
    }

    #[test]
    fn test_parse_select_step_some() {
        let select = SelectSpec::from_cli_args(&Some(vec!["one".to_string(), "two".to_string()]));
        let step = parse_select_step(&select).expect("should return some");
        assert_eq!(step.columns.len(), 2);
        assert_eq!(step.columns[0], ColumnSpec::Exact("one".into()));
        assert_eq!(step.columns[1], ColumnSpec::Exact("two".into()));
    }

    #[test]
    fn test_parse_select_step_comma_separated() {
        let select = SelectSpec::from_cli_args(&Some(vec!["one, two".to_string()]));
        let step = parse_select_step(&select).expect("should return some");
        assert_eq!(step.columns.len(), 2);
        assert_eq!(step.columns[0], ColumnSpec::Exact("one".into()));
        assert_eq!(step.columns[1], ColumnSpec::Exact("two".into()));
    }

    #[test]
    fn test_parse_select_step_empty_returns_none() {
        let select = SelectSpec::from_cli_args(&Some(vec!["  ,  ".to_string()]));
        assert!(parse_select_step(&select).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_select_columns() {
        // Use the parquet reader to inspect the file and verify column selection
        let args = ReadArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: None,
            offset: None,
            csv_has_header: None,
        };
        let parquet_step = ReadParquetStep { args };

        let source: RecordBatchReaderSource = Box::new(parquet_step);
        let select_step = SelectColumnsStep {
            columns: vec![
                ColumnSpec::Exact("two".to_string()),
                ColumnSpec::Exact("four".to_string()),
            ],
        };
        let mut projected_source = select_step
            .execute(source)
            .await
            .expect("Failed to execute select columns");
        let mut projected_reader = projected_source
            .get()
            .expect("Failed to get record batch reader");

        // 1. Check Schema
        let projected_schema = projected_reader.schema();
        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "two");
        assert_eq!(projected_schema.field(1).name(), "four");

        // 2. Check Data
        let batch_result = projected_reader.next().unwrap();
        let projected_batch = batch_result.unwrap();
        let batch_rows = projected_batch.num_rows();

        assert_eq!(projected_batch.num_columns(), 2);
        assert_eq!(projected_batch.column(0).len(), batch_rows);
        assert_eq!(projected_batch.column(1).len(), batch_rows);
    }
}
