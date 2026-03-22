//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod batch_write;
pub mod csv;
pub mod dataframe;
pub mod datasource;
pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod read;
pub mod record_batch_filter;
pub mod schema;
pub mod select;
pub mod write;
pub mod xlsx;
pub mod yaml;

use std::any::Any;
use std::path::Path;

use arrow::array::RecordBatchReader;
use arrow::array::UInt32Array;
use arrow::compute;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelineExecutionError;
use crate::errors::PipelinePlanningError;
use crate::pipeline::avro::ReadAvroStep;
use crate::pipeline::csv::ReadCsvStep;
use crate::pipeline::dataframe::DataFrameReader;
use crate::pipeline::dataframe::DataFrameSource;
use crate::pipeline::orc::ReadOrcStep;
use crate::pipeline::parquet::ReadParquetStep;
pub use crate::pipeline::read::ReadArgs;
pub use crate::pipeline::write::WriteArgs;
pub use crate::pipeline::write::WriteJsonArgs;
pub use crate::pipeline::write::WriteYamlArgs;

#[derive(Default)]
pub struct PipelineBuilder {
    read: Option<String>,
    select: Option<Vec<ColumnSpec>>,
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
    count: Option<String>,
    write: Option<String>,
    schema: bool,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PipelineBuilder {
    pub fn read(&mut self, path: &str) -> &mut Self {
        self.read = Some(path.to_string());
        self
    }
    pub fn select(&mut self, columns: &[&str]) -> &mut Self {
        self.select = Some(
            columns
                .iter()
                .map(|c| ColumnSpec::Exact(c.to_string()))
                .collect(),
        );
        self
    }
    pub fn write<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.write = Some(path.as_ref().to_string_lossy().to_string());
        self
    }
    pub fn head(&mut self, n: usize) -> &mut Self {
        self.head = Some(n);
        self
    }
    pub fn tail(&mut self, n: usize) -> &mut Self {
        self.tail = Some(n);
        self
    }
    pub fn sample(&mut self, n: usize) -> &mut Self {
        self.sample = Some(n);
        self
    }
    pub fn count(&mut self, path: &str) -> &mut Self {
        self.count = Some(path.to_string());
        self
    }
    pub fn schema(&mut self) -> &mut Self {
        self.schema = true;
        self
    }

    pub fn build(&mut self) -> crate::Result<Box<dyn Pipeline + 'static>> {
        let input_path = self.read.as_ref().ok_or_else(|| {
            Error::PipelinePlanningError(PipelinePlanningError::MissingRequiredStage(
                "read(path)".to_string(),
            ))
        })?;
        let output_path = self.write.as_ref().ok_or_else(|| {
            Error::PipelinePlanningError(PipelinePlanningError::MissingRequiredStage(
                "write(path)".to_string(),
            ))
        })?;

        if self.tail.is_some() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedStage("tail(n)".to_string()),
            ));
        }
        if self.sample.is_some() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedStage("sample(n)".to_string()),
            ));
        }
        if self.count.is_some() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedStage("count(path?)".to_string()),
            ));
        }
        if self.schema {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedStage("schema()".to_string()),
            ));
        }

        let input_file_type = crate::resolve_file_type(None, input_path)?;
        let output_file_type = crate::resolve_file_type(None, output_path)?;

        let is_input_native = matches!(
            input_file_type,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json
        );
        let is_output_native = matches!(
            output_file_type,
            FileType::Parquet | FileType::Csv | FileType::Json
        );

        if !is_input_native {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }
        if !is_output_native {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedOutputFileType(output_file_type.to_string()),
            ));
        }

        let select = self.select.clone();
        if let Some(columns) = &select
            && columns.is_empty()
        {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::SelectEmpty,
            ));
        }

        Ok(Box::new(DataFramePipeline {
            input_path: input_path.to_string(),
            input_file_type,
            select,
            head: self.head,
            output_path: output_path.to_string(),
            output_file_type,
        }))
    }
}

pub trait Pipeline: Any {
    fn execute(&mut self) -> crate::Result<()>;
}

pub struct DataFramePipeline {
    input_path: String,
    input_file_type: FileType,
    select: Option<Vec<ColumnSpec>>,
    head: Option<usize>,
    output_path: String,
    output_file_type: FileType,
}

/// How to match a column name: exact (case-sensitive) or case-insensitive.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// Exact match (from string literal like "column").
    Exact(String),
    /// Case-insensitive match (from symbol like :column or bare identifier).
    CaseInsensitive(String),
}

/// Macro to create a vector of ColumnSpec from a list of column names.
#[macro_export]
macro_rules! column_spec {
    ( $($col:expr),+ $(,)? ) => {
        vec![
            $(
                $crate::pipeline::ColumnSpec::Exact($col.to_string())
            ),+
        ]
    };
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

impl Pipeline for DataFramePipeline {
    fn execute(&mut self) -> crate::Result<()> {
        let input_path = self.input_path.clone();
        let input_file_type = self.input_file_type;
        let select = self.select.clone();
        let head = self.head;
        let output_path = self.output_path.clone();
        let output_file_type = self.output_file_type;

        let fut = async move {
            let ctx = SessionContext::new();

            let mut df = match input_file_type {
                FileType::Parquet => ctx
                    .read_parquet(&input_path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Avro => ctx
                    .read_avro(&input_path, AvroReadOptions::default())
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Csv => ctx
                    .read_csv(&input_path, CsvReadOptions::new().has_header(true))
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Json => ctx
                    .read_json(&input_path, NdJsonReadOptions::default())
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                other => {
                    return Err(Error::PipelineExecutionError(
                        PipelineExecutionError::UnsupportedInputFileType(other),
                    ));
                }
            };

            if let Some(columns) = &select
                && !columns.is_empty()
            {
                let schema = df.schema();
                let resolved: Vec<String> = columns
                    .iter()
                    .map(|c| c.resolve(schema.as_ref()))
                    .collect::<crate::Result<_>>()?;
                let col_refs: Vec<&str> = resolved.iter().map(String::as_str).collect();
                df = df
                    .select_columns(&col_refs)
                    .map_err(|e| Error::GenericError(e.to_string()))?;
            }

            if let Some(n) = head {
                df = df
                    .limit(0, Some(n))
                    .map_err(|e| Error::GenericError(e.to_string()))?;
            }

            let write_opts = DataFrameWriteOptions::new();
            let _ = match output_file_type {
                FileType::Parquet => df
                    .write_parquet(&output_path, write_opts, None)
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Csv => df
                    .write_csv(&output_path, write_opts, None)
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Json => df
                    .write_json(&output_path, write_opts, None)
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                other => {
                    return Err(Error::PipelineExecutionError(
                        PipelineExecutionError::UnsupportedOutputFileType(other),
                    ));
                }
            };

            Ok::<(), crate::Error>(())
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(fut)
        } else {
            // If `Pipeline::execute` is called outside a tokio runtime, spin one up.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| Error::GenericError(e.to_string()))?;
            rt.block_on(fut)
        }
    }
}

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
#[async_trait(?Send)]
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    async fn execute(self, input: Self::Input) -> Result<Self::Output>;
}

/// A source that yields a value of type `T`.
pub trait Source<T: ?Sized> {
    /// Produces the next value from this source; consumes the source on first call.
    fn get(&mut self) -> Result<Box<T>>;
}

/// Type alias for a boxed source of `RecordBatchReader`.
pub type RecordBatchReaderSource = Box<dyn Source<dyn RecordBatchReader + 'static>>;

/// A RecordBatchReader that yields batches from a Vec.
pub struct VecRecordBatchReader {
    batches: Vec<arrow::record_batch::RecordBatch>,
    index: usize,
}

impl VecRecordBatchReader {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self { batches, index: 0 }
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.batches.len() {
            return None;
        }
        let batch = self.batches[self.index].clone();
        self.index += 1;
        Some(Ok(batch))
    }
}

impl RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()))
    }
}

/// A concrete implementation of Source<dyn RecordBatchReader + 'static> that yiedls a VecRecordBatchReader.
pub struct VecRecordBatchReaderSource {
    batches: Option<Vec<arrow::record_batch::RecordBatch>>,
}

impl VecRecordBatchReaderSource {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self {
            batches: Some(batches),
        }
    }
}

impl Source<dyn RecordBatchReader + 'static> for VecRecordBatchReaderSource {
    fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        let batches = std::mem::take(&mut self.batches)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))?;
        Ok(Box::new(VecRecordBatchReader { batches, index: 0 }))
    }
}

/// Builds a format-specific `RecordBatchReaderSource` for the given file type.
pub fn build_reader(
    path: &str,
    file_type: FileType,
    limit: Option<usize>,
    offset: Option<usize>,
    csv_has_header: Option<bool>,
) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match file_type {
        FileType::Parquet => Box::new(ReadParquetStep {
            args: ReadArgs {
                path: path.to_string(),
                limit,
                offset,
                csv_has_header: None,
            },
        }),
        FileType::Avro => Box::new(ReadAvroStep {
            args: ReadArgs {
                path: path.to_string(),
                limit,
                offset,
                csv_has_header: None,
            },
        }),
        FileType::Csv => Box::new(ReadCsvStep {
            path: path.to_string(),
            has_header: csv_has_header,
            limit,
        }),
        FileType::Orc => Box::new(ReadOrcStep {
            args: ReadArgs {
                path: path.to_string(),
                limit,
                offset,
                csv_has_header: None,
            },
        }),
        _ => {
            return Err(crate::Error::GenericError(format!(
                "Unsupported file type for reading: {file_type}"
            )));
        }
    };
    Ok(reader)
}

/// Counts rows in a file. Uses metadata for Parquet and ORC (no data read);
/// streams batches for Avro and CSV.
pub fn count_rows(path: &str, file_type: FileType, csv_has_header: Option<bool>) -> Result<usize> {
    if matches!(file_type, FileType::Parquet | FileType::Orc) {
        return crate::get_total_rows_result(path, file_type);
    }
    let mut reader_step = build_reader(path, file_type, None, None, csv_has_header)?;
    let reader = reader_step.get()?;
    let mut total = 0usize;
    for batch in reader {
        let batch = batch.map_err(crate::Error::from)?;
        total += batch.num_rows();
    }
    Ok(total)
}

/// A `RecordBatchReader` that wraps a `DataFrameSource`, lazily streaming
/// batches from the underlying DataFrame via `execute_stream()`.
pub struct DataFrameToBatchReader {
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataFrameToBatchReader {
    pub async fn try_new(mut source: DataFrameSource) -> Result<Self> {
        let df = *source.get()?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| crate::Error::GenericError(e.to_string()))?;
        let schema = stream.schema();
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            schema,
            stream,
            handle,
        })
    }

    pub fn into_batches(self) -> Vec<arrow::record_batch::RecordBatch> {
        self.filter_map(|r| r.ok()).collect()
    }
}

impl Iterator for DataFrameToBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.clone();
        tokio::task::block_in_place(|| handle.block_on(self.stream.next()))
            .map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for DataFrameToBatchReader {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Reads input into record batches for use by REPL and other callers that need RecordBatchReaderSource.
/// Uses DataFusion for Parquet, Avro, and CSV; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
    csv_has_header: Option<bool>,
) -> eyre::Result<Vec<arrow::record_batch::RecordBatch>> {
    let source = {
        let select = select.clone();
        DataFrameReader::new(input_path, input_file_type, select, limit, csv_has_header)
    }
    .execute(())
    .await?;
    let reader = DataFrameToBatchReader::try_new(source)
        .await
        .map_err(|e| eyre::eyre!("{e}"))?;
    Ok(reader.into_batches())
}

/// Takes the last `n` rows from a sequence of record batches.
pub fn tail_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    n: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let number = n.min(total_rows);
    let skip = total_rows.saturating_sub(number);

    let mut result = Vec::new();
    let mut rows_emitted = 0usize;
    let mut rows_skipped = 0usize;
    for batch in batches {
        let batch_rows = batch.num_rows();
        if rows_skipped + batch_rows <= skip {
            rows_skipped += batch_rows;
            continue;
        }
        let start_in_batch = skip.saturating_sub(rows_skipped);
        rows_skipped += start_in_batch;
        let take = (number - rows_emitted).min(batch_rows - start_in_batch);
        if take == 0 {
            break;
        }
        result.push(batch.slice(start_in_batch, take));
        rows_emitted += take;
    }

    result
}

/// Extracts pre-chosen rows from a single batch given sorted global indices.
/// Advances `idx_pos` past all consumed indices and returns the selected rows (if any).
fn take_rows_at_sorted_indices(
    batch: &RecordBatch,
    batch_start: usize,
    indices: &[usize],
    idx_pos: &mut usize,
) -> Option<RecordBatch> {
    let batch_end = batch_start + batch.num_rows();
    let mut local_indices: Vec<u32> = Vec::new();
    while *idx_pos < indices.len() && indices[*idx_pos] < batch_end {
        local_indices.push((indices[*idx_pos] - batch_start) as u32);
        *idx_pos += 1;
    }
    if local_indices.is_empty() {
        return None;
    }
    let index_array = UInt32Array::from(local_indices);
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col, &index_array, None).expect("take failed"))
        .collect();
    Some(RecordBatch::try_new(batch.schema(), columns).expect("RecordBatch::try_new failed"))
}

/// Samples `n` random rows from in-memory batches. Used by the REPL.
pub fn sample_batches(batches: Vec<RecordBatch>, n: usize) -> Vec<RecordBatch> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if n >= total_rows {
        return batches;
    }
    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> = rand::seq::index::sample(&mut rng, total_rows, n).into_vec();
    indices.sort_unstable();

    let mut result = Vec::new();
    let mut batch_start = 0usize;
    let mut idx_pos = 0usize;
    for batch in &batches {
        if idx_pos >= indices.len() {
            break;
        }
        if let Some(selected) =
            take_rows_at_sorted_indices(batch, batch_start, &indices, &mut idx_pos)
        {
            result.push(selected);
        }
        batch_start += batch.num_rows();
    }
    result
}

/// Samples `n` random rows by streaming through a reader when `total_rows` is known
/// (Parquet, ORC). Generates sorted random indices upfront, then picks rows batch-by-batch
/// without holding all records in memory.
pub fn sample_from_reader(
    reader: Box<dyn RecordBatchReader + 'static>,
    total_rows: usize,
    n: usize,
) -> Vec<RecordBatch> {
    let effective_n = n.min(total_rows);
    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> =
        rand::seq::index::sample(&mut rng, total_rows, effective_n).into_vec();
    indices.sort_unstable();

    let mut result = Vec::new();
    let mut batch_start = 0usize;
    let mut idx_pos = 0usize;
    for batch_result in reader {
        if idx_pos >= indices.len() {
            break;
        }
        let batch = match batch_result {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(selected) =
            take_rows_at_sorted_indices(&batch, batch_start, &indices, &mut idx_pos)
        {
            result.push(selected);
        }
        batch_start += batch.num_rows();
    }
    result
}

/// Samples `n` rows from a reader using reservoir sampling (Algorithm L).
/// Used for formats where the total row count is unknown (Avro, CSV).
pub fn reservoir_sample_from_reader(
    reader: Box<dyn RecordBatchReader + 'static>,
    n: usize,
) -> Vec<RecordBatch> {
    let schema = reader.schema();
    let row_iter = reader.filter_map(|r| r.ok()).flat_map(|batch| {
        let num = batch.num_rows();
        (0..num).map(move |i| Some(batch.slice(i, 1)))
    });
    let mut sample: Vec<Option<RecordBatch>> = vec![None; n];
    reservoir_sampling::unweighted::l(row_iter, &mut sample);
    let sampled: Vec<RecordBatch> = sample.into_iter().flatten().collect();
    if sampled.is_empty() {
        return sampled;
    }
    match compute::concat_batches(&schema, &sampled) {
        Ok(merged) => vec![merged],
        Err(_) => sampled,
    }
}

/// Writes record batches to output file. Used by REPL.
pub async fn write_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> eyre::Result<()> {
    let ctx = datafusion::execution::context::SessionContext::new();
    let df = ctx.read_batches(batches).map_err(|e| eyre::eyre!("{e}"))?;

    let source = crate::pipeline::dataframe::DataFrameSource::new(df);
    let writer_step = crate::pipeline::dataframe::DataFrameWriter::new(
        output_path,
        output_file_type,
        sparse,
        json_pretty,
    );
    writer_step.execute(source).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;

    use super::*;
    use crate::pipeline::ColumnSpec;
    use crate::pipeline::ReadArgs;
    use crate::pipeline::RecordBatchReaderSource;
    use crate::pipeline::parquet::ReadParquetStep;

    #[test]
    fn test_pipeline_builder_read_select_write_parquet() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.parquet")
            .select(&["one", "two"])
            .write("output.parquet");
        let pipeline = builder.build().unwrap();
        assert!((pipeline.as_ref() as &dyn Any).is::<DataFramePipeline>());
        let pipeline = (pipeline.as_ref() as &dyn Any)
            .downcast_ref::<DataFramePipeline>()
            .expect("pipeline is a DataFramePipeline");
        assert_eq!(FileType::Parquet, pipeline.input_file_type);
        assert_eq!(Some(column_spec!("one", "two")), pipeline.select);
        assert_eq!(FileType::Parquet, pipeline.output_file_type);
    }

    #[test]
    fn test_pipeline_builder_read_select_write_csv() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.parquet")
            .select(&["one", "two"])
            .write("output.csv");

        let pipeline = builder.build().expect("Failed to build pipeline");
        assert!((pipeline.as_ref() as &dyn Any).is::<DataFramePipeline>());
        let pipeline = (pipeline.as_ref() as &dyn Any)
            .downcast_ref::<DataFramePipeline>()
            .expect("pipeline is a DataFramePipeline");
        assert_eq!(FileType::Parquet, pipeline.input_file_type);
        assert_eq!(Some(column_spec!("one", "two")), pipeline.select);
        assert_eq!(FileType::Csv, pipeline.output_file_type);
    }

    #[test]
    fn test_column_spec_resolve() {
        let schema = Schema::new(vec![
            Field::new("one", DataType::Int32, false),
            Field::new("two", DataType::Int32, false),
        ]);
        let spec = ColumnSpec::Exact("one".to_string());
        let name = spec.resolve(&schema).unwrap();
        assert_eq!(name, "one");

        let spec = ColumnSpec::CaseInsensitive("ONE".to_string());
        let name = spec.resolve(&schema).unwrap();
        assert_eq!(name, "one");
    }
}
