//! The `pipeline` module is the core of the datu crate.

pub mod avro;
pub mod csv;
pub mod dataframe;

pub use avro::DataframeAvroReader;
pub use avro::DataframeAvroWriter;
pub use avro::RecordBatchAvroWriter;
pub use csv::DataframeCsvReader;
pub use csv::DataframeCsvWriter;
pub use csv::RecordBatchCsvWriter;
pub use dataframe::DataFramePipeline;
pub use dataframe::DataFrameSink;
pub use dataframe::DataFrameSource;
pub use dataframe::DataframeHead;
pub use dataframe::DataframeSample;
pub use dataframe::DataframeSelect;
pub use dataframe::DataframeTail;
pub use dataframe::DataframeToRecordBatch;
pub use dataframe::DataframeToRecordBatchProducer;
pub use json::DataframeJsonReader;
pub use json::DataframeJsonWriter;
pub use json::RecordBatchJsonWriter;
pub use orc::OrcRecordBatchReader;
pub use orc::RecordBatchOrcWriter;
pub use parquet::DataframeParquetReader;
pub use parquet::DataframeParquetWriter;
pub use parquet::RecordBatchParquetWriter;
pub use record_batch::RecordBatchHead;
pub use record_batch::RecordBatchPipeline;
pub use record_batch::RecordBatchSample;
pub use record_batch::RecordBatchSelect;
pub use record_batch::RecordBatchSink;
pub use record_batch::RecordBatchTail;

pub mod display;
pub mod json;
pub mod orc;
pub mod parquet;
pub mod read;
pub mod record_batch;
pub mod schema;
pub mod select;
pub mod write;
pub mod xlsx;
pub mod yaml;

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::array::UInt32Array;
use arrow::compute;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::cli::DisplayOutputFormat;
use crate::errors::PipelinePlanningError;
use crate::get_total_rows_result;
use crate::pipeline;
use crate::pipeline::avro::RecordBatchAvroReader;
use crate::pipeline::csv::ReadCsvStepRecordBatch;
use crate::pipeline::dataframe::LegacyDataFrameReader;
use crate::pipeline::parquet::RecordBatchParquetReader;
use crate::pipeline::read::ReadArgs;
use crate::resolve_file_type;

fn slice_from_head_tail_sample(
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
) -> Option<DisplaySlice> {
    sample
        .map(DisplaySlice::Sample)
        .or_else(|| tail.map(DisplaySlice::Tail))
        .or_else(|| head.map(DisplaySlice::Head))
}

fn ensure_at_most_one_of_head_tail_sample(
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
) -> Result<()> {
    let slice_count =
        usize::from(head.is_some()) + usize::from(tail.is_some()) + usize::from(sample.is_some());
    if slice_count > 1 {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedStage(
                "only one of head(n), tail(n), or sample(n) may be set".to_string(),
            ),
        ));
    }
    Ok(())
}

/// Fluent builder for a [`Pipeline`] (file conversion or head/tail/sample display).
pub struct PipelineBuilder {
    read: Option<String>,
    select: Option<SelectSpec>,
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
    count: Option<String>,
    write: Option<String>,
    schema: bool,
    input_type_override: Option<FileType>,
    output_type_override: Option<FileType>,
    csv_has_header: Option<bool>,
    sparse: bool,
    json_pretty: bool,
    progress: Option<ProgressBar>,
    display_output_format: Option<DisplayOutputFormat>,
    display_csv_headers: Option<bool>,
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self {
            read: None,
            select: None,
            head: None,
            tail: None,
            sample: None,
            count: None,
            write: None,
            schema: false,
            input_type_override: None,
            output_type_override: None,
            csv_has_header: None,
            sparse: true,
            json_pretty: false,
            progress: None,
            display_output_format: None,
            display_csv_headers: None,
        }
    }
}

impl PipelineBuilder {
    /// Returns a builder with default options (no paths set).
    pub fn new() -> Self {
        Self::default()
    }
}

impl PipelineBuilder {
    /// Sets the input file path (required before [`build`](PipelineBuilder::build)).
    pub fn read(&mut self, path: &str) -> &mut Self {
        self.read = Some(path.to_string());
        self
    }
    /// Sets column selection as exact name matches.
    pub fn select(&mut self, columns: &[&str]) -> &mut Self {
        self.select = Some(SelectSpec {
            columns: columns
                .iter()
                .map(|c| ColumnSpec::Exact(c.to_string()))
                .collect(),
        });
        self
    }

    /// Sets column selection from a pre-built spec (e.g. from CLI conversion).
    pub fn select_spec(&mut self, spec: SelectSpec) -> &mut Self {
        self.select = Some(spec);
        self
    }
    /// Sets the output path for conversion (mutually exclusive with display-only mode).
    pub fn write<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.write = Some(path.as_ref().to_string_lossy().to_string());
        self
    }
    /// Keeps the first `n` rows (with write or stdout display).
    pub fn head(&mut self, n: usize) -> &mut Self {
        self.head = Some(n);
        self
    }
    /// Keeps the last `n` rows (with write or stdout display).
    pub fn tail(&mut self, n: usize) -> &mut Self {
        self.tail = Some(n);
        self
    }
    /// Random sample of `n` rows (with write or stdout display).
    pub fn sample(&mut self, n: usize) -> &mut Self {
        self.sample = Some(n);
        self
    }
    /// Sets a row-count path (not supported by [`build`](PipelineBuilder::build); use the CLI).
    pub fn count(&mut self, path: &str) -> &mut Self {
        self.count = Some(path.to_string());
        self
    }
    /// Requests schema output (not supported by [`build`](PipelineBuilder::build); use the CLI).
    pub fn schema(&mut self) -> &mut Self {
        self.schema = true;
        self
    }

    /// Override input format (e.g. from CLI `-I`); otherwise inferred from the read path extension.
    pub fn input_type(&mut self, file_type: Option<FileType>) -> &mut Self {
        self.input_type_override = file_type;
        self
    }

    /// Override output format (e.g. from CLI `-O`); otherwise inferred from the write path extension.
    pub fn output_type(&mut self, file_type: Option<FileType>) -> &mut Self {
        self.output_type_override = file_type;
        self
    }

    /// When reading CSV: whether the first row is a header (`None` means default true).
    pub fn csv_has_header(&mut self, has_header: Option<bool>) -> &mut Self {
        self.csv_has_header = has_header;
        self
    }

    /// For JSON/YAML and similar writers: omit null keys when true.
    pub fn sparse(&mut self, sparse: bool) -> &mut Self {
        self.sparse = sparse;
        self
    }

    /// When writing JSON: pretty-print when true.
    pub fn json_pretty(&mut self, json_pretty: bool) -> &mut Self {
        self.json_pretty = json_pretty;
        self
    }

    /// Optional progress bar updated while writing from collected batches.
    pub fn progress(&mut self, progress: Option<ProgressBar>) -> &mut Self {
        self.progress = progress;
        self
    }

    /// When building a display pipeline (no `write`): stdout format (defaults to CSV if unset).
    pub fn display_format(&mut self, format: DisplayOutputFormat) -> &mut Self {
        self.display_output_format = Some(format);
        self
    }

    /// When building a display pipeline (no `write`): whether CSV output includes a header row
    /// (defaults to true if unset).
    pub fn display_csv_headers(&mut self, headers: bool) -> &mut Self {
        self.display_csv_headers = Some(headers);
        self
    }

    /// Builds a write pipeline from the builder's configuration.
    fn build_write_pipeline(
        &self,
        input_path: &str,
        output_path: &str,
        select: Option<SelectSpec>,
    ) -> Result<Pipeline> {
        ensure_at_most_one_of_head_tail_sample(self.head, self.tail, self.sample)?;
        let input_file_type = resolve_file_type(self.input_type_override, input_path)?;
        let output_file_type = resolve_file_type(self.output_type_override, output_path)?;

        let is_input_native = matches!(
            input_file_type,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json | FileType::Orc
        );
        let is_output_supported = matches!(
            output_file_type,
            FileType::Parquet
                | FileType::Csv
                | FileType::Json
                | FileType::Orc
                | FileType::Avro
                | FileType::Xlsx
                | FileType::Yaml
        );

        if !is_input_native {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }
        if !is_output_supported {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedOutputFileType(output_file_type.to_string()),
            ));
        }

        let slice = slice_from_head_tail_sample(self.head, self.tail, self.sample);
        if input_file_type == FileType::Orc {
            Ok(Pipeline::RecordBatch(Box::new(RecordBatchPipeline {
                input_path: input_path.to_string(),
                input_file_type,
                select,
                slice,
                sparse: self.sparse,
                sink: RecordBatchSink::Write {
                    output_path: output_path.to_string(),
                    output_file_type,
                    json_pretty: self.json_pretty,
                    progress: self.progress.clone(),
                },
            })))
        } else {
            Ok(Pipeline::DataFrame(Box::new(DataFramePipeline {
                input_path: input_path.to_string(),
                input_file_type,
                select,
                slice,
                csv_has_header: self.csv_has_header,
                sparse: self.sparse,
                sink: DataFrameSink::Write {
                    output_path: output_path.to_string(),
                    output_file_type,
                    json_pretty: self.json_pretty,
                    progress: self.progress.clone(),
                },
            })))
        }
    }

    /// Builds a display pipeline from the builder's configuration.
    fn build_display_pipeline(
        &self,
        input_path: &str,
        select: Option<SelectSpec>,
    ) -> Result<Pipeline> {
        ensure_at_most_one_of_head_tail_sample(self.head, self.tail, self.sample)?;
        let slice =
            slice_from_head_tail_sample(self.head, self.tail, self.sample).ok_or_else(|| {
                Error::PipelinePlanningError(PipelinePlanningError::MissingRequiredStage(
                    "head(n), tail(n), or sample(n)".to_string(),
                ))
            })?;
        let input_file_type = resolve_file_type(self.input_type_override, input_path)?;
        let is_display_input = matches!(
            input_file_type,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc | FileType::Json
        );
        if !is_display_input {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }

        let output_format = self
            .display_output_format
            .unwrap_or(DisplayOutputFormat::Csv);
        let csv_stdout_headers = self.display_csv_headers.unwrap_or(true);

        let input_path = input_path.to_string();
        let csv_has_header = self.csv_has_header;
        let sparse = self.sparse;

        if input_file_type == FileType::Orc {
            Ok(Pipeline::RecordBatch(Box::new(RecordBatchPipeline {
                input_path,
                input_file_type,
                select,
                slice: Some(slice),
                sparse,
                sink: RecordBatchSink::Display {
                    output_format,
                    csv_stdout_headers,
                },
            })))
        } else {
            Ok(Pipeline::DataFrame(Box::new(DataFramePipeline {
                input_path,
                input_file_type,
                select,
                slice: Some(slice),
                csv_has_header,
                sparse,
                sink: DataFrameSink::Display {
                    output_format,
                    csv_stdout_headers,
                },
            })))
        }
    }

    /// Consumes configuration and returns a [`Pipeline`] or a planning error.
    pub fn build(&mut self) -> Result<Pipeline> {
        let input_path = self.read.as_ref().ok_or_else(|| {
            Error::PipelinePlanningError(PipelinePlanningError::MissingRequiredStage(
                "read(path)".to_string(),
            ))
        })?;

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

        let select = self.select.clone();
        if let Some(spec) = &select
            && spec.is_empty()
        {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::SelectEmpty,
            ));
        }

        if let Some(output_path) = &self.write {
            self.build_write_pipeline(input_path, output_path, select)
        } else {
            self.build_display_pipeline(input_path, select)
        }
    }
}

/// Result of [`PipelineBuilder::build`]: DataFusion path vs ORC record-batch path (see `PIPELINE.mermaid`).
pub enum Pipeline {
    DataFrame(Box<DataFramePipeline>),
    RecordBatch(Box<RecordBatchPipeline>),
}

impl Pipeline {
    /// Runs read → select → head/tail/sample → sink (file or stdout).
    pub fn execute(&mut self) -> Result<()> {
        match self {
            Pipeline::DataFrame(p) => p.execute(),
            Pipeline::RecordBatch(p) => p.execute(),
        }
    }
}

/// Head, tail, or random sample of rows to send to stdout for a display pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplaySlice {
    Head(usize),
    Tail(usize),
    Sample(usize),
}

/// How to match a column name: exact (case-sensitive) or case-insensitive.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// Exact match (from string literal like "column").
    Exact(String),
    /// Case-insensitive match (from symbol like :column or bare identifier).
    CaseInsensitive(String),
}

/// Macro to build a [`SelectSpec`] of [`ColumnSpec::Exact`] entries from column names.
#[macro_export]
macro_rules! select_spec {
    ( $($col:expr),+ $(,)? ) => {
        $crate::pipeline::SelectSpec {
            columns: vec![
                $(
                    $crate::pipeline::ColumnSpec::Exact($col.to_string())
                ),+
            ],
        }
    };
}

impl ColumnSpec {
    /// Resolves this spec against a schema, returning the actual column name.
    pub fn resolve(&self, schema: &Schema) -> Result<String> {
        match self {
            ColumnSpec::Exact(name) => schema
                .index_of(name)
                .map(|_| name.clone())
                .map_err(|e| Error::GenericError(format!("Column '{name}' not found: {e}"))),
            ColumnSpec::CaseInsensitive(name) => schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(name))
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    Error::GenericError(format!(
                        "Column '{name}' not found (case-insensitive match)"
                    ))
                }),
        }
    }
}

/// Column selection: ordered list of [`ColumnSpec`] entries (e.g. from CLI or REPL).
#[derive(Clone, Debug, PartialEq)]
pub struct SelectSpec {
    pub columns: Vec<ColumnSpec>,
}

impl SelectSpec {
    /// Returns true when the selection has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Parses CLI `--select` values from `Option<Vec<String>>`: splits each string on commas,
    /// trims, drops empties, maps to [`ColumnSpec::Exact`]. Returns `None` when `select` is `None`
    /// or yields no column names.
    pub fn from_cli_args(select: &Option<Vec<String>>) -> Option<Self> {
        let inner = select.as_ref()?;
        let mut columns = Vec::new();
        for s in inner {
            columns.extend(s.split(',').filter_map(|c| {
                let c = c.trim();
                if c.is_empty() {
                    None
                } else {
                    Some(ColumnSpec::Exact(c.to_string()))
                }
            }));
        }
        if columns.is_empty() {
            None
        } else {
            Some(Self { columns })
        }
    }

    /// Resolves all column specs against `schema`, returning actual column names in order.
    pub fn resolve_names(&self, schema: &Schema) -> Result<Vec<String>> {
        pipeline::select::resolve_column_specs(schema, &self.columns)
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

/// A producer produces a value of type `T`.
#[async_trait(?Send)]
pub trait Producer<T: ?Sized> {
    /// Produces the next value from this source; consumes the source on first call.
    async fn get(&mut self) -> Result<Box<T>>;
}

/// Type alias for a boxed source of `RecordBatchReader`.
pub type RecordBatchReaderSource = Box<dyn Producer<dyn RecordBatchReader + 'static>>;

/// A RecordBatchReader that yields batches from a Vec.
pub struct VecRecordBatchReader {
    batches: Vec<arrow::record_batch::RecordBatch>,
    index: usize,
}

impl VecRecordBatchReader {
    /// Creates a reader that iterates `batches` in order.
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

pub(crate) struct ProgressVecRecordBatchReader {
    inner: VecRecordBatchReader,
    progress: Option<ProgressBar>,
}

impl Iterator for ProgressVecRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if let Ok(ref batch) = item
            && let Some(pb) = &self.progress
        {
            pb.inc(batch.num_rows() as u64);
        }
        Some(item)
    }
}

impl RecordBatchReader for ProgressVecRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// A concrete implementation of Source<dyn RecordBatchReader + 'static> that yiedls a VecRecordBatchReader.
pub struct VecRecordBatchReaderSource {
    batches: Option<Vec<arrow::record_batch::RecordBatch>>,
}

impl VecRecordBatchReaderSource {
    /// Wraps `batches` in a one-shot [`Producer`] of [`VecRecordBatchReader`].
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self {
            batches: Some(batches),
        }
    }
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for VecRecordBatchReaderSource {
    async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        let batches = std::mem::take(&mut self.batches)
            .ok_or_else(|| Error::GenericError("Reader already taken".to_string()))?;
        Ok(Box::new(VecRecordBatchReader { batches, index: 0 }))
    }
}

/// Builds a format-specific `RecordBatchReaderSource` for [`ReadArgs::file_type`].
pub fn build_reader(args: &ReadArgs) -> Result<RecordBatchReaderSource> {
    let reader: RecordBatchReaderSource = match args.file_type {
        FileType::Parquet => Box::new(RecordBatchParquetReader { args: args.clone() }),
        FileType::Avro => Box::new(RecordBatchAvroReader { args: args.clone() }),
        FileType::Csv => Box::new(ReadCsvStepRecordBatch { args: args.clone() }),
        FileType::Orc => Box::new(OrcRecordBatchReader { args: args.clone() }),
        _ => {
            return Err(Error::GenericError(format!(
                "Unsupported file type for reading: {}",
                args.file_type
            )));
        }
    };
    Ok(reader)
}

/// Counts rows in a file. Uses metadata for Parquet and ORC (no data read);
/// streams batches for Avro and CSV.
pub async fn count_rows(
    path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<usize> {
    if matches!(file_type, FileType::Parquet | FileType::Orc) {
        return get_total_rows_result(path, file_type);
    }
    let mut read_args = ReadArgs::new(path, file_type);
    read_args.csv_has_header = csv_has_header;
    let mut reader_step = build_reader(&read_args)?;
    let reader = reader_step.get().await?;
    let mut total = 0usize;
    for batch in reader {
        let batch = batch.map_err(Error::from)?;
        total += batch.num_rows();
    }
    Ok(total)
}

/// Reads input into record batches for use by REPL and other callers that need RecordBatchReaderSource.
/// Uses DataFusion for Parquet, Avro, and CSV; uses orc-rust for ORC.
pub async fn read_to_batches(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<SelectSpec>,
    limit: Option<usize>,
    csv_has_header: Option<bool>,
) -> eyre::Result<Vec<arrow::record_batch::RecordBatch>> {
    let source = {
        let select = select.clone();
        LegacyDataFrameReader::new(input_path, input_file_type, select, limit, csv_has_header)
    }
    .execute(())
    .await?;
    let reader = pipeline::dataframe::DataframeToRecordBatch::try_new(source)
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

    let source = pipeline::dataframe::DataFrameSource::new(df);
    let writer_step = pipeline::dataframe::DataFrameWriter::new(
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
    use tempfile::NamedTempFile;

    use super::*;
    use crate::pipeline::ColumnSpec;
    use crate::pipeline::SelectSpec;
    use crate::pipeline::avro::DataframeAvroWriter;
    use crate::pipeline::avro::get_schema_fields_avro;
    use crate::pipeline::parquet::DataframeParquetReader;
    use crate::pipeline::read::ReadArgs;
    use crate::pipeline::write::WriteArgs;

    #[test]
    fn test_pipeline_builder_read_select_write_parquet() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.parquet")
            .select(&["one", "two"])
            .write("output.parquet");
        let built = builder.build().unwrap();
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(FileType::Parquet, p.input_file_type);
        assert_eq!(Some(select_spec!("one", "two")), p.select);
        let DataFrameSink::Write {
            output_file_type, ..
        } = &p.sink
        else {
            panic!("expected write sink");
        };
        assert_eq!(FileType::Parquet, *output_file_type);
    }

    #[test]
    fn test_pipeline_builder_read_select_write_csv() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.parquet")
            .select(&["one", "two"])
            .write("output.csv");

        let built = builder.build().expect("Failed to build pipeline");
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(FileType::Parquet, p.input_file_type);
        assert_eq!(Some(select_spec!("one", "two")), p.select);
        let DataFrameSink::Write {
            output_file_type, ..
        } = &p.sink
        else {
            panic!("expected write sink");
        };
        assert_eq!(FileType::Csv, *output_file_type);
    }

    #[test]
    fn test_pipeline_builder_read_orc_write_csv() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.orc")
            .select(&["col"])
            .write("output.csv");
        let built = builder.build().unwrap();
        let Pipeline::RecordBatch(p) = built else {
            panic!("expected RecordBatch pipeline");
        };
        assert_eq!(Some(select_spec!("col")), p.select);
        let RecordBatchSink::Write {
            output_file_type, ..
        } = &p.sink
        else {
            panic!("expected write sink");
        };
        assert_eq!(FileType::Csv, *output_file_type);
    }

    #[test]
    fn test_pipeline_builder_read_parquet_write_orc() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("input.parquet")
            .select(&["one"])
            .write("output.orc");
        let built = builder.build().unwrap();
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(FileType::Parquet, p.input_file_type);
        assert_eq!(Some(select_spec!("one")), p.select);
        let DataFrameSink::Write {
            output_file_type, ..
        } = &p.sink
        else {
            panic!("expected write sink");
        };
        assert_eq!(FileType::Orc, *output_file_type);
    }

    #[test]
    fn test_record_batch_pipeline_execute_orc_to_csv() {
        let temp = tempfile::tempdir().unwrap();
        let out = temp.path().join("out.csv");
        let mut builder = PipelineBuilder::new();
        builder
            .read("fixtures/userdata.orc")
            .write(out.to_str().expect("utf8 path"));
        let mut built = builder.build().expect("build pipeline");
        let Pipeline::RecordBatch(ref mut p) = built else {
            panic!("expected RecordBatch pipeline");
        };
        p.execute().expect("execute pipeline");
        assert!(out.is_file());
    }

    #[test]
    fn test_dataframe_pipeline_execute_avro_to_orc() {
        let temp = tempfile::tempdir().unwrap();
        let out = temp.path().join("out.orc");
        let mut builder = PipelineBuilder::new();
        builder
            .read("fixtures/userdata5.avro")
            .select(&["id", "first_name"])
            .write(out.to_str().expect("utf8 path"));
        let mut built = builder.build().expect("build pipeline");
        let Pipeline::DataFrame(ref mut p) = built else {
            panic!("expected DataFrame pipeline");
        };
        p.execute().expect("execute pipeline");
        assert!(out.is_file());
    }

    #[test]
    fn test_pipeline_builder_read_head_display_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").head(3);
        let built = builder.build().expect("build display pipeline");
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(FileType::Parquet, p.input_file_type);
        assert_eq!(p.slice, Some(DisplaySlice::Head(3)));
        assert!(p.select.is_none());
        assert!(matches!(p.sink, DataFrameSink::Display { .. }));
    }

    #[test]
    fn test_pipeline_builder_read_head_display_orc_uses_record_batch_pipeline() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/userdata.orc").head(3);
        let built = builder.build().expect("build display pipeline");
        let Pipeline::RecordBatch(p) = built else {
            panic!("expected RecordBatch pipeline");
        };
        assert_eq!(FileType::Orc, p.input_file_type);
        assert_eq!(p.slice, Some(DisplaySlice::Head(3)));
        assert!(matches!(p.sink, RecordBatchSink::Display { .. }));
    }

    #[test]
    fn test_pipeline_builder_read_sample_display_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").sample(2);
        let built = builder.build().expect("build display pipeline");
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(FileType::Parquet, p.input_file_type);
        assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
    }

    #[test]
    fn test_pipeline_builder_read_sample_display_orc_uses_record_batch_pipeline() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/userdata.orc").sample(2);
        let built = builder.build().expect("build display pipeline");
        let Pipeline::RecordBatch(p) = built else {
            panic!("expected RecordBatch pipeline");
        };
        assert_eq!(FileType::Orc, p.input_file_type);
        assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
    }

    #[test]
    fn test_pipeline_builder_read_sample_write_csv() {
        let mut builder = PipelineBuilder::new();
        builder
            .read("fixtures/table.parquet")
            .write("out.csv")
            .sample(2);
        let built = builder
            .build()
            .expect("conversion with sample should build");
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
    }

    #[test]
    fn test_pipeline_builder_display_rejects_head_and_sample() {
        use crate::Error;
        use crate::errors::PipelinePlanningError;

        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").head(2).sample(3);
        let err = match builder.build() {
            Ok(_) => panic!("head and sample should fail"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            Error::PipelinePlanningError(PipelinePlanningError::UnsupportedStage(ref s))
                if s == "only one of head(n), tail(n), or sample(n) may be set"
        ));
    }

    #[test]
    fn test_record_batch_display_pipeline_execute_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").head(2);
        let mut built = builder.build().expect("build display pipeline");
        built.execute().expect("execute display pipeline");
    }

    #[test]
    fn test_pipeline_builder_read_tail_display_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").tail(2);
        let built = builder.build().expect("build tail display pipeline");
        let Pipeline::DataFrame(p) = built else {
            panic!("expected DataFrame pipeline");
        };
        assert_eq!(p.slice, Some(DisplaySlice::Tail(2)));
    }

    #[test]
    fn test_record_batch_display_pipeline_execute_tail_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").tail(2);
        let mut built = builder.build().expect("build tail display pipeline");
        built.execute().expect("execute tail display pipeline");
    }

    #[test]
    fn test_record_batch_display_pipeline_execute_head_orc() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/userdata.orc").head(2);
        let mut built = builder.build().expect("build orc display pipeline");
        let Pipeline::RecordBatch(_) = &built else {
            panic!("expected RecordBatch pipeline");
        };
        built.execute().expect("execute orc display pipeline");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_display_pipeline_execute_sample_parquet() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/table.parquet").sample(2);
        let mut built = builder.build().expect("build sample display pipeline");
        built.execute().expect("execute sample display pipeline");
    }

    #[test]
    fn test_record_batch_display_pipeline_execute_sample_orc() {
        let mut builder = PipelineBuilder::new();
        builder.read("fixtures/userdata.orc").sample(2);
        let mut built = builder.build().expect("build orc sample display pipeline");
        let Pipeline::RecordBatch(_) = &built else {
            panic!("expected RecordBatch pipeline");
        };
        built
            .execute()
            .expect("execute orc sample display pipeline");
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

    #[test]
    fn test_select_spec_from_cli_args_none() {
        assert!(SelectSpec::from_cli_args(&None).is_none());
    }

    #[test]
    fn test_select_spec_from_cli_args_parsing() {
        assert!(SelectSpec::from_cli_args(&Some(vec![])).is_none());
        let spec = SelectSpec::from_cli_args(&Some(vec!["a".to_string(), "b".to_string()]))
            .expect("non-empty");
        assert_eq!(
            spec.columns,
            vec![ColumnSpec::Exact("a".into()), ColumnSpec::Exact("b".into()),]
        );
        let spec = SelectSpec::from_cli_args(&Some(vec!["a, b".to_string(), "c".to_string()]))
            .expect("non-empty");
        assert_eq!(
            spec.columns,
            vec![
                ColumnSpec::Exact("a".into()),
                ColumnSpec::Exact("b".into()),
                ColumnSpec::Exact("c".into()),
            ]
        );
        let spec =
            SelectSpec::from_cli_args(&Some(vec![" one ,  two  ".to_string()])).expect("non-empty");
        assert_eq!(
            spec.columns,
            vec![
                ColumnSpec::Exact("one".into()),
                ColumnSpec::Exact("two".into()),
            ]
        );
    }

    #[test]
    fn test_select_spec_from_cli_args_empty_fragments() {
        assert!(SelectSpec::from_cli_args(&Some(vec!["  ,  ".to_string()])).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_parquet_write_avro_steps() {
        let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let read_step = DataframeParquetReader { args: read_args };
        let tempfile = NamedTempFile::with_suffix(".avro").expect("Failed to create temp file");
        let write_args = WriteArgs {
            path: tempfile
                .path()
                .to_str()
                .expect("Failed to get path")
                .to_string(),
            file_type: FileType::Avro,
            sparse: None,
            pretty: None,
        };
        let write_step = DataframeAvroWriter { args: write_args };
        write_step
            .execute(Box::new(read_step))
            .await
            .expect("Failed to write Avro file");
        assert!(tempfile.path().exists());
        let schema = get_schema_fields_avro(tempfile.path().to_str().expect("Failed to get path"))
            .expect("Failed to get schema fields");
        assert_eq!(schema.len(), 6, "Expected 6 columns");
        assert_eq!(schema[0].name, "one", "Expected first column name is 'one'");
        assert_eq!(schema[0].data_type, "Float64", "Expected Float64 data type");
        assert!(schema[0].nullable, "Expected nullable column");
    }
}
