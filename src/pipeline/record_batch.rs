use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
use crate::pipeline::ColumnSpec;
use crate::pipeline::DisplaySlice;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::avro::RecordBatchAvroWriter;
use crate::pipeline::csv::RecordBatchCsvWriter;
use crate::pipeline::display::apply_select_and_display;
use crate::pipeline::json::RecordBatchJsonWriter;
use crate::pipeline::orc::OrcRecordBatchReader;
use crate::pipeline::orc::RecordBatchOrcWriter;
use crate::pipeline::parquet::RecordBatchParquetWriter;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::sample_from_reader;
use crate::pipeline::select::resolve_column_specs;
use crate::pipeline::tail_batches;
use crate::pipeline::write as write_dispatch;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteJsonArgs;

/// A Source that wraps a single RecordBatchReader and yields it on get().
struct RecordBatchReaderHolder {
    reader: Option<Box<dyn RecordBatchReader + 'static>>,
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for RecordBatchReaderHolder {
    async fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        std::mem::take(&mut self.reader)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))
    }
}

/// Pipeline step that filters record batches to only the specified columns.
pub struct RecordBatchSelect {
    pub columns: Vec<ColumnSpec>,
}

#[async_trait(?Send)]
impl Step for RecordBatchSelect {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let reader = input.get().await?;
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

/// Builds a [`RecordBatchSelect`] from a resolved [`SelectSpec`].
/// Returns None when no selection is requested.
pub fn parse_select_step(select: &Option<SelectSpec>) -> Option<RecordBatchSelect> {
    let spec = select.as_ref()?;
    if spec.is_empty() {
        return None;
    }
    Some(RecordBatchSelect {
        columns: spec.columns.clone(),
    })
}

/// Skips the first `offset_remaining` logical rows from an underlying [`RecordBatchReader`].
pub(crate) struct SkipRowsRecordBatchReader {
    reader: Box<dyn RecordBatchReader + 'static>,
    offset_remaining: usize,
}

impl RecordBatchReader for SkipRowsRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

impl Iterator for SkipRowsRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = self.reader.next()?;
            let batch = match batch {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            let batch_rows = batch.num_rows();
            if batch_rows == 0 {
                continue;
            }
            if self.offset_remaining == 0 {
                return Some(Ok(batch));
            }
            if batch_rows <= self.offset_remaining {
                self.offset_remaining -= batch_rows;
                continue;
            }
            let start = self.offset_remaining;
            self.offset_remaining = 0;
            return Some(Ok(batch.slice(start, batch_rows - start)));
        }
    }
}

/// Yields at most `remaining` rows from an underlying [`RecordBatchReader`].
/// Used by [`RecordBatchHead`] and by [`apply_offset_limit`] for record-batch read limits.
pub(crate) struct TakeRowsRecordBatchReader {
    reader: Box<dyn RecordBatchReader + 'static>,
    remaining: usize,
}

impl RecordBatchReader for TakeRowsRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

impl Iterator for TakeRowsRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        loop {
            match self.reader.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(batch)) => {
                    let rows = batch.num_rows();
                    if rows == 0 {
                        continue;
                    }
                    if rows <= self.remaining {
                        self.remaining -= rows;
                        return Some(Ok(batch));
                    }
                    let slice = batch.slice(0, self.remaining);
                    self.remaining = 0;
                    return Some(Ok(slice));
                }
            }
        }
    }
}

/// Applies SQL-style `OFFSET` / `LIMIT` across record batches: skip `offset` rows, then take at most
/// `limit` rows when `limit` is `Some`.
pub(crate) fn apply_offset_limit(
    reader: Box<dyn RecordBatchReader + 'static>,
    offset: usize,
    limit: Option<usize>,
) -> Box<dyn RecordBatchReader + 'static> {
    let mut r = reader;
    if offset > 0 {
        r = Box::new(SkipRowsRecordBatchReader {
            reader: r,
            offset_remaining: offset,
        });
    }
    if let Some(n) = limit {
        r = Box::new(TakeRowsRecordBatchReader {
            reader: r,
            remaining: n,
        });
    }
    r
}

/// Keeps the first `n` rows across batches (same streaming cap as record-batch read `limit` after any skip).
pub struct RecordBatchHead {
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for RecordBatchHead {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let reader = input.get().await?;
        let wrapped = Box::new(TakeRowsRecordBatchReader {
            reader,
            remaining: self.n,
        });
        Ok(Box::new(RecordBatchReaderHolder {
            reader: Some(wrapped),
        }))
    }
}

/// Keeps the last `n` rows (materializes all batches). Orthogonal to [`ReadArgs`] offset/limit from
/// the start of the file; use for tail/slice display, not for SQL-style `OFFSET`/`LIMIT`.
pub struct RecordBatchTail {
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for RecordBatchTail {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let reader = input.get().await?;
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::ArrowError)?;
        let batches = tail_batches(batches, self.n);
        Ok(Box::new(VecRecordBatchReaderSource::new(batches)))
    }
}

/// Random sample of `n` rows (uses ORC row count from `input_path`).
pub struct RecordBatchSample {
    pub input_path: String,
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for RecordBatchSample {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let total_rows = crate::get_total_rows_result(&self.input_path, FileType::Orc)?;
        let reader = input.get().await?;
        let sampled = sample_from_reader(reader, total_rows, self.n);
        Ok(Box::new(VecRecordBatchReaderSource::new(sampled)))
    }
}

/// File output vs stdout display; tail of [`RecordBatchPipeline`] after ORC read/select/slice.
pub enum RecordBatchSink {
    Write {
        output_path: String,
        output_file_type: FileType,
        json_pretty: bool,
        #[allow(dead_code)]
        progress: Option<ProgressBar>,
    },
    Display {
        output_format: DisplayOutputFormat,
        csv_stdout_headers: bool,
    },
}

/// ORC input via record-batch steps (see `PIPELINE.mermaid` RecordBatch branch).
pub struct RecordBatchPipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    pub(crate) slice: Option<DisplaySlice>,
    pub(crate) sparse: bool,
    pub(crate) sink: RecordBatchSink,
}

impl RecordBatchPipeline {
    /// ORC read, optional column select, optional head/tail/sample, then [`RecordBatchSink`].
    pub fn execute(&mut self) -> crate::Result<()> {
        if self.input_file_type != FileType::Orc {
            return Err(Error::GenericError(format!(
                "RecordBatchPipeline only supports ORC input, got {}",
                self.input_file_type
            )));
        }

        let input_path = self.input_path.clone();
        let select = self.select.clone();
        let slice = self.slice;
        let sparse = self.sparse;
        let sink = match &self.sink {
            RecordBatchSink::Write {
                output_path,
                output_file_type,
                json_pretty,
                progress,
            } => RecordBatchSink::Write {
                output_path: output_path.clone(),
                output_file_type: *output_file_type,
                json_pretty: *json_pretty,
                progress: progress.clone(),
            },
            RecordBatchSink::Display {
                output_format,
                csv_stdout_headers,
            } => RecordBatchSink::Display {
                output_format: *output_format,
                csv_stdout_headers: *csv_stdout_headers,
            },
        };

        let fut = async move {
            let read_args = ReadArgs::new(input_path.clone(), FileType::Orc);
            let mut source: RecordBatchReaderSource =
                Box::new(OrcRecordBatchReader { args: read_args });

            if let Some(select_step) = parse_select_step(&select) {
                source = select_step.execute(source).await?;
            }

            if let Some(slice) = slice {
                source = match slice {
                    DisplaySlice::Head(n) => RecordBatchHead { n }.execute(source).await?,
                    DisplaySlice::Tail(n) => RecordBatchTail { n }.execute(source).await?,
                    DisplaySlice::Sample(n) => {
                        RecordBatchSample {
                            input_path: input_path.clone(),
                            n,
                        }
                        .execute(source)
                        .await?
                    }
                };
            }

            match sink {
                RecordBatchSink::Write {
                    output_path,
                    output_file_type,
                    json_pretty,
                    progress: _progress,
                } => {
                    let write_args = WriteArgs {
                        path: output_path.clone(),
                        file_type: output_file_type,
                        sparse: Some(sparse),
                        pretty: Some(json_pretty),
                    };

                    match output_file_type {
                        FileType::Parquet => {
                            RecordBatchParquetWriter {
                                args: write_args,
                                source,
                            }
                            .execute(())
                            .await?;
                        }
                        FileType::Csv => {
                            RecordBatchCsvWriter {
                                args: write_args,
                                source,
                            }
                            .execute(())
                            .await?;
                        }
                        FileType::Json => {
                            let args = WriteJsonArgs {
                                path: output_path,
                                sparse,
                                pretty: json_pretty,
                            };
                            let path = args.path.as_str();
                            let mut reader = source.get().await?;
                            let writer = RecordBatchJsonWriter::from_write_json_args(&args);
                            writer.write_to_path(&mut *reader, path)?;
                        }
                        FileType::Avro => {
                            RecordBatchAvroWriter { args: write_args }
                                .execute(source)
                                .await?;
                        }
                        FileType::Orc => {
                            RecordBatchOrcWriter {
                                args: write_args,
                                source,
                            }
                            .execute(())
                            .await?;
                        }
                        FileType::Xlsx | FileType::Yaml => {
                            write_dispatch::write_record_batches(source, write_args).await?;
                        }
                    }
                }
                RecordBatchSink::Display {
                    output_format,
                    csv_stdout_headers,
                } => {
                    apply_select_and_display(
                        source,
                        None,
                        output_format,
                        sparse,
                        csv_stdout_headers,
                    )
                    .await?;
                }
            }

            Ok::<(), Error>(())
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

/// Per-format sink adapter for writing record batches.
pub trait BatchWriteSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> crate::Result<()>;
    fn finish(self) -> crate::Result<()>;
}

/// Shared harness for batch-oriented file writers.
pub fn write_record_batches_with_sink<S, BuildSink>(
    path: &str,
    reader: &mut dyn RecordBatchReader,
    build_sink: BuildSink,
) -> crate::Result<()>
where
    S: BatchWriteSink,
    BuildSink: FnOnce(&str, SchemaRef) -> crate::Result<S>,
{
    let schema = reader.schema();
    let mut sink = build_sink(path, schema)?;

    for batch in reader {
        let batch = batch.map_err(Error::ArrowError)?;
        sink.write_batch(&batch)?;
    }

    sink.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileType;
    use crate::pipeline::ColumnSpec;
    use crate::pipeline::RecordBatchReaderSource;
    use crate::pipeline::SelectSpec;
    use crate::pipeline::parquet::RecordBatchParquetReader;
    use crate::pipeline::read::ReadArgs;

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
        let args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let parquet_step = RecordBatchParquetReader { args };

        let source: RecordBatchReaderSource = Box::new(parquet_step);
        let select_step = RecordBatchSelect {
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
            .await
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
