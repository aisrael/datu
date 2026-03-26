use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::DataFrame;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use futures::StreamExt;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
use crate::errors::PipelineExecutionError;
use crate::pipeline::DisplaySlice;
use crate::pipeline::Producer;
use crate::pipeline::ProgressVecRecordBatchReader;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::avro;
use crate::pipeline::csv::DataframeCsvWriter;
use crate::pipeline::display;
use crate::pipeline::display::DisplayWriterStep;
use crate::pipeline::json::DataframeJsonWriter;
use crate::pipeline::orc;
use crate::pipeline::parquet;
use crate::pipeline::reservoir_sample_from_reader;
use crate::pipeline::sample_from_reader;
use crate::pipeline::tail_batches;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::xlsx;

/// A source that yields a DataFusion DataFrame, implementing `Source<DataFrame>`.
pub struct DataFrameSource {
    pub(crate) df: Option<DataFrame>,
}

impl std::fmt::Debug for DataFrameSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFrameSource")
            .field("df", &self.df)
            .finish()
    }
}

impl DataFrameSource {
    pub fn new(df: datafusion::dataframe::DataFrame) -> Self {
        Self { df: Some(df) }
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataFrameSource {
    async fn get(&mut self) -> crate::Result<Box<datafusion::dataframe::DataFrame>> {
        let df = self
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        Ok(Box::new(df))
    }
}

/// A step that writes a DataFusion DataFrame to an output file.
pub struct DataFrameWriter {
    output_path: String,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
}

impl DataFrameWriter {
    pub fn new<S: Into<String>>(
        output_path: S,
        output_file_type: FileType,
        sparse: bool,
        json_pretty: bool,
    ) -> Self {
        Self {
            output_path: output_path.into(),
            output_file_type,
            sparse,
            json_pretty,
        }
    }
}

/// Writes record batches from a reader to an output file. Used by DataFrameWriter and the convert
/// command to eliminate duplicate format dispatch.
pub fn write_record_batches_from_reader(
    reader: &mut dyn RecordBatchReader,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> crate::Result<()> {
    if output_file_type != FileType::Json && json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }

    match output_file_type {
        FileType::Parquet => parquet::write_record_batches(output_path, reader)?,
        FileType::Csv => crate::pipeline::csv::write_record_batches(output_path, reader)?,
        FileType::Json => {
            let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
            if json_pretty {
                display::write_record_batches_as_json_pretty(reader, file, sparse)?;
            } else {
                display::write_record_batches_as_json(reader, file, sparse)?;
            }
        }
        FileType::Yaml => {
            let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
            display::write_record_batches_as_yaml(reader, file, sparse)?;
        }
        FileType::Avro => avro::write_record_batches(output_path, reader)?,
        FileType::Orc => orc::write_record_batches(output_path, reader)?,
        FileType::Xlsx => xlsx::write_record_batch_to_xlsx(output_path, reader)?,
    }

    Ok(())
}

#[async_trait(?Send)]
impl Step for DataFrameWriter {
    type Input = DataFrameSource;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input.get().await?;

        let handle = tokio::runtime::Handle::current();
        let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))
            .map_err(|e| Error::GenericError(e.to_string()))?;

        let mut reader = VecRecordBatchReader::new(batches);
        write_record_batches_from_reader(
            &mut reader,
            &self.output_path,
            self.output_file_type,
            self.sparse,
            self.json_pretty,
        )
    }
}

/// A step that reads an input file into a DataFusion DataFrame with optional column selection and limit.
pub struct LegacyDataFrameReader {
    input_path: String,
    input_file_type: FileType,
    select: Option<SelectSpec>,
    limit: Option<usize>,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    csv_has_header: Option<bool>,
}

impl LegacyDataFrameReader {
    pub fn new(
        input_path: &str,
        input_file_type: FileType,
        select: Option<SelectSpec>,
        limit: Option<usize>,
        csv_has_header: Option<bool>,
    ) -> Self {
        Self {
            input_path: input_path.to_string(),
            input_file_type,
            select,
            limit,
            csv_has_header,
        }
    }

    async fn read(&self) -> crate::Result<DataFrameSource> {
        let ctx = SessionContext::new();

        let mut df = match self.input_file_type {
            FileType::Parquet => ctx
                .read_parquet(&self.input_path, ParquetReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Avro => ctx
                .read_avro(&self.input_path, AvroReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Csv => {
                let has_header = self.csv_has_header.unwrap_or(true);
                ctx.read_csv(
                    &self.input_path,
                    CsvReadOptions::new().has_header(has_header),
                )
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?
            }
            FileType::Json => ctx
                .read_json(&self.input_path, NdJsonReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Orc => {
                let batches = read_orc_to_batches(&self.input_path)?;
                if batches.is_empty() {
                    return Err(Error::GenericError(
                        "ORC file is empty or could not be read".to_string(),
                    ));
                }
                ctx.read_batches(batches)
                    .map_err(|e| Error::GenericError(e.to_string()))?
            }
            _ => {
                return Err(Error::GenericError(
                    "Only Parquet, Avro, CSV, JSON, and ORC are supported as input file types"
                        .to_string(),
                ));
            }
        };

        if let Some(spec) = &self.select
            && !spec.is_empty()
        {
            let schema = df.schema();
            let resolved = spec.resolve_names(schema.as_ref())?;
            let col_refs: Vec<&str> = resolved.iter().map(String::as_str).collect();
            df = df
                .select_columns(&col_refs)
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }

        if let Some(n) = self.limit {
            df = df
                .limit(0, Some(n))
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }

        Ok(DataFrameSource::new(df))
    }
}

#[async_trait(?Send)]
impl Step for LegacyDataFrameReader {
    type Input = ();
    type Output = DataFrameSource;

    async fn execute(self, _input: Self::Input) -> crate::Result<Self::Output> {
        self.read().await
    }
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion after reading.
fn read_orc_to_batches(path: &str) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
    orc::read_orc_all_batches(path)
}

/// A `RecordBatchReader` that wraps a `DataFrameSource`, lazily streaming batches from the
/// underlying DataFrame via `execute_stream()`.
pub struct DataframeToRecordBatch {
    schema: Arc<arrow::datatypes::Schema>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataframeToRecordBatch {
    pub async fn try_new(mut source: DataFrameSource) -> crate::Result<Self> {
        let df = *source.get().await?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?;
        let schema = stream.schema();
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            schema,
            stream,
            handle,
        })
    }

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

/// Yields a [`DataframeToRecordBatch`] once from a [`DataFrameSource`] (for chaining to [`crate::pipeline::avro::RecordBatchAvroWriter`]).
pub struct DataframeToRecordBatchProducer {
    inner: Option<DataFrameSource>,
}

impl DataframeToRecordBatchProducer {
    pub fn new(source: DataFrameSource) -> Self {
        Self {
            inner: Some(source),
        }
    }
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for DataframeToRecordBatchProducer {
    async fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        let source = self
            .inner
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame source already taken".to_string()))?;
        let reader = DataframeToRecordBatch::try_new(source).await?;
        Ok(Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Applies optional column selection to a [`DataFrame`].
pub fn dataframe_apply_select(
    mut df: DataFrame,
    select: Option<&SelectSpec>,
) -> crate::Result<DataFrame> {
    if let Some(spec) = select
        && !spec.is_empty()
    {
        let schema = df.schema();
        let resolved = spec.resolve_names(schema.as_ref())?;
        let col_refs: Vec<&str> = resolved.iter().map(String::as_str).collect();
        df = df
            .select_columns(&col_refs)
            .map_err(|e| Error::GenericError(e.to_string()))?;
    }
    Ok(df)
}

/// Keeps the first `n` rows (same semantics as [`DataFrame::limit`] with offset 0).
pub fn dataframe_apply_head(df: DataFrame, n: usize) -> crate::Result<DataFrame> {
    df.limit(0, Some(n))
        .map_err(|e| Error::GenericError(e.to_string()))
}

/// Keeps the last `n` rows; strategy matches [`DataFramePipeline`] display mode.
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
            let df = df
                .limit(skip, Some(number))
                .map_err(|e| Error::GenericError(e.to_string()))?;
            Ok(df)
        }
        FileType::Csv | FileType::Json | FileType::Avro | FileType::Orc => {
            let all = df
                .collect()
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?;
            let batches = tail_batches(all, tail_n);
            SessionContext::new()
                .read_batches(batches)
                .map_err(|e| Error::GenericError(e.to_string()))
        }
        other => Err(Error::GenericError(format!(
            "DataFrame tail is not supported for input type: {other}"
        ))),
    }
}

/// Random sample of `n` rows; strategy matches [`DataFramePipeline`] display mode.
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
            SessionContext::new()
                .read_batches(batches)
                .map_err(|e| Error::GenericError(e.to_string()))
        }
        FileType::Avro | FileType::Csv | FileType::Json | FileType::Orc => {
            let source = DataFrameSource::new(df);
            let batch_reader = DataframeToRecordBatch::try_new(source).await?;
            let batches = reservoir_sample_from_reader(Box::new(batch_reader), sample_n);
            SessionContext::new()
                .read_batches(batches)
                .map_err(|e| Error::GenericError(e.to_string()))
        }
        other => Err(Error::GenericError(format!(
            "DataFrame sample is not supported for input type: {other}"
        ))),
    }
}

/// Optional column projection on a [`DataFrameSource`].
pub struct DataframeSelect {
    pub select: Option<SelectSpec>,
}

#[async_trait(?Send)]
impl Step for DataframeSelect {
    type Input = DataFrameSource;
    type Output = DataFrameSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        let df = dataframe_apply_select(df, self.select.as_ref())?;
        Ok(DataFrameSource::new(df))
    }
}

/// Keeps the first `n` rows of a [`DataFrameSource`].
pub struct DataframeHead {
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for DataframeHead {
    type Input = DataFrameSource;
    type Output = DataFrameSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        let df = dataframe_apply_head(df, self.n)?;
        Ok(DataFrameSource::new(df))
    }
}

/// Keeps the last `n` rows (requires original file path and type for Parquet row counts).
pub struct DataframeTail {
    pub input_path: String,
    pub input_file_type: FileType,
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for DataframeTail {
    type Input = DataFrameSource;
    type Output = DataFrameSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        let df = dataframe_apply_tail(df, &self.input_path, self.input_file_type, self.n).await?;
        Ok(DataFrameSource::new(df))
    }
}

/// Random sample of `n` rows.
pub struct DataframeSample {
    pub input_path: String,
    pub input_file_type: FileType,
    pub n: usize,
}

#[async_trait(?Send)]
impl Step for DataframeSample {
    type Input = DataFrameSource;
    type Output = DataFrameSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let df = input
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        let df = dataframe_apply_sample(df, &self.input_path, self.input_file_type, self.n).await?;
        Ok(DataFrameSource::new(df))
    }
}

/// File output vs stdout display; the tail of [`DataFramePipeline`] after read/select/slice.
pub enum DataFrameSink {
    Write {
        output_path: String,
        output_file_type: FileType,
        json_pretty: bool,
        progress: Option<ProgressBar>,
    },
    Display {
        output_format: DisplayOutputFormat,
        csv_stdout_headers: bool,
    },
}

/// DataFusion-based pipeline: Parquet, Avro, CSV, JSON (not ORC; see [`crate::pipeline::RecordBatchPipeline`]).
pub struct DataFramePipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    pub(crate) slice: Option<DisplaySlice>,
    pub(crate) csv_has_header: Option<bool>,
    pub(crate) sparse: bool,
    pub(crate) sink: DataFrameSink,
}

impl DataFramePipeline {
    /// Read, optional column select, optional head/tail/sample, then [`DataFrameSink`].
    pub fn execute(&mut self) -> crate::Result<()> {
        let input_path = self.input_path.clone();
        let input_file_type = self.input_file_type;
        let select = self.select.clone();
        let slice = self.slice;
        let csv_has_header = self.csv_has_header;
        let sparse = self.sparse;
        let sink = match &self.sink {
            DataFrameSink::Write {
                output_path,
                output_file_type,
                json_pretty,
                progress,
            } => DataFrameSink::Write {
                output_path: output_path.clone(),
                output_file_type: *output_file_type,
                json_pretty: *json_pretty,
                progress: progress.clone(),
            },
            DataFrameSink::Display {
                output_format,
                csv_stdout_headers,
            } => DataFrameSink::Display {
                output_format: *output_format,
                csv_stdout_headers: *csv_stdout_headers,
            },
        };

        let fut = async move {
            let mut source = dataframe_pipeline_prepare_source(
                input_path,
                input_file_type,
                select,
                slice,
                csv_has_header,
            )
            .await?;

            match sink {
                DataFrameSink::Write {
                    output_path,
                    output_file_type,
                    json_pretty,
                    progress,
                } => {
                    let write_args = WriteArgs {
                        path: output_path.clone(),
                        file_type: output_file_type,
                        sparse: Some(sparse),
                        pretty: Some(json_pretty),
                    };

                    match output_file_type {
                        FileType::Parquet => {
                            parquet::DataframeParquetWriter { args: write_args }
                                .execute(Box::new(source))
                                .await?;
                        }
                        FileType::Csv => {
                            DataframeCsvWriter { args: write_args }
                                .execute(Box::new(source))
                                .await?;
                        }
                        FileType::Json => {
                            DataframeJsonWriter { args: write_args }
                                .execute(Box::new(source))
                                .await?;
                        }
                        FileType::Avro => {
                            avro::DataframeAvroWriter { args: write_args }
                                .execute(Box::new(source))
                                .await?;
                        }
                        FileType::Orc | FileType::Xlsx | FileType::Yaml => {
                            let df = source.df.take().ok_or_else(|| {
                                Error::GenericError("DataFrame already taken".to_string())
                            })?;
                            let batches = df
                                .collect()
                                .await
                                .map_err(|e| Error::GenericError(e.to_string()))?;
                            let inner = VecRecordBatchReader::new(batches);
                            let mut reader = ProgressVecRecordBatchReader { inner, progress };
                            write_record_batches_from_reader(
                                &mut reader,
                                &output_path,
                                output_file_type,
                                sparse,
                                json_pretty,
                            )?;
                        }
                    }
                }
                DataFrameSink::Display {
                    output_format,
                    csv_stdout_headers,
                } => {
                    let df = source.df.take().ok_or_else(|| {
                        Error::GenericError("DataFrame already taken".to_string())
                    })?;
                    let batches = df
                        .collect()
                        .await
                        .map_err(|e| Error::GenericError(e.to_string()))?;
                    let source = Box::new(VecRecordBatchReaderSource::new(batches));
                    let display_step = DisplayWriterStep {
                        output_format,
                        sparse,
                        headers: csv_stdout_headers,
                    };
                    display_step.execute(source).await?;
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

/// Read into a [`DataFrameSource`], apply optional column select, then optional head/tail/sample.
pub(crate) async fn dataframe_pipeline_prepare_source(
    input_path: String,
    input_file_type: FileType,
    select: Option<SelectSpec>,
    slice: Option<DisplaySlice>,
    csv_has_header: Option<bool>,
) -> crate::Result<DataFrameSource> {
    let ctx = SessionContext::new();
    let has_header = csv_has_header.unwrap_or(true);

    let df = match input_file_type {
        FileType::Parquet => ctx
            .read_parquet(&input_path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?,
        FileType::Avro => ctx
            .read_avro(&input_path, AvroReadOptions::default())
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?,
        FileType::Csv => ctx
            .read_csv(&input_path, CsvReadOptions::new().has_header(has_header))
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

    let mut source = DataFrameSource::new(df);

    source = DataframeSelect { select }.execute(source).await?;

    if let Some(slice) = slice {
        source = match slice {
            DisplaySlice::Head(n) => DataframeHead { n }.execute(source).await?,
            DisplaySlice::Tail(tail_n) => {
                DataframeTail {
                    input_path: input_path.clone(),
                    input_file_type,
                    n: tail_n,
                }
                .execute(source)
                .await?
            }
            DisplaySlice::Sample(n) => {
                DataframeSample {
                    input_path: input_path.clone(),
                    input_file_type,
                    n,
                }
                .execute(source)
                .await?
            }
        };
    }

    Ok(source)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::DataframeSelect;
    use super::DataframeTail;
    use super::DataframeToRecordBatchProducer;
    use crate::FileType;
    use crate::pipeline::Producer;
    use crate::pipeline::RecordBatchAvroWriter;
    use crate::pipeline::SelectSpec;
    use crate::pipeline::Step;
    use crate::pipeline::csv::DataframeCsvWriter;
    use crate::pipeline::dataframe::DataFrameWriter;
    use crate::pipeline::dataframe::LegacyDataFrameReader;
    use crate::pipeline::parquet::DataframeParquetReader;
    use crate::pipeline::read::ReadArgs;
    use crate::pipeline::read_to_batches;
    use crate::pipeline::write::WriteArgs;
    use crate::pipeline::write_batches;

    async fn count_rows(df: datafusion::dataframe::DataFrame) -> usize {
        let mut total_rows: usize = 0;
        let mut stream = df.execute_stream().await.unwrap();
        while let Some(batch) = stream.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        total_rows
    }

    fn temp_path(dir: &tempfile::TempDir, name: &str) -> String {
        dir.path()
            .join(name)
            .to_str()
            .expect("Failed to convert path to string")
            .to_string()
    }

    // --- DataFrameReader tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe() {
        let df = *LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select() {
        let select = SelectSpec::from_cli_args(&Some(vec!["one".to_string(), "two".to_string()]));
        let df = *LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["one", "two"]);
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_limit() {
        let df = *LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            Some(2),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select_and_limit() {
        let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string()]));
        let df = *LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            Some(1),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["two"]);
        assert_eq!(count_rows(df).await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_avro() {
        let df = *LegacyDataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            None,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_orc() {
        let df = *LegacyDataFrameReader::new(
            "fixtures/userdata.orc",
            FileType::Orc,
            None,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_csv() {
        let df =
            *LegacyDataFrameReader::new("fixtures/table.csv", FileType::Csv, None, Some(2), None)
                .execute(())
                .await
                .unwrap()
                .get()
                .await
                .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_unsupported_type() {
        let result =
            LegacyDataFrameReader::new("fixtures/table.parquet", FileType::Xlsx, None, None, None)
                .execute(())
                .await;
        match result {
            Ok(_) => panic!("Expected error, got Ok"),
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("Only Parquet, Avro, CSV, JSON, and ORC")
            ),
        }
    }

    // --- write_dataframe tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_parquet() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        DataFrameWriter::new(&output, FileType::Parquet, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *LegacyDataFrameReader::new(&output, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_csv() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        DataFrameWriter::new(&output, FileType::Csv, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        DataFrameWriter::new(&output, FileType::Json, true, false)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json_pretty() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        DataFrameWriter::new(&output, FileType::Json, true, true)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains('\n'));
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_yaml() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.yaml");
        DataFrameWriter::new(&output, FileType::Yaml, true, false)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_avro() {
        let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string(), "three".to_string()]));
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.avro");
        DataFrameWriter::new(&output, FileType::Avro, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *LegacyDataFrameReader::new(&output, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_orc() {
        let select =
            SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
        let source = LegacyDataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            select,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.orc");
        DataFrameWriter::new(&output, FileType::Orc, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *LegacyDataFrameReader::new(&output, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_xlsx() {
        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.xlsx");
        DataFrameWriter::new(&output, FileType::Xlsx, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    // --- read_to_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_parquet() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_with_select_and_limit() {
        let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string()]));
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &select,
            Some(2),
            None,
        )
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "two");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_avro() {
        let batches = read_to_batches(
            "fixtures/userdata5.avro",
            FileType::Avro,
            &None,
            Some(3),
            None,
        )
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_orc() {
        let batches = read_to_batches("fixtures/userdata.orc", FileType::Orc, &None, Some(3), None)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- write_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_csv() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        write_batches(batches, &output, FileType::Csv, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_json() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_batches(batches, &output, FileType::Json, true, false)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_parquet() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        write_batches(batches, &output, FileType::Parquet, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let roundtrip = read_to_batches(&output, FileType::Parquet, &None, None, None)
            .await
            .unwrap();
        let total_rows: usize = roundtrip.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- round-trip tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_parquet_avro_parquet() {
        let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string(), "three".to_string()]));
        let temp_dir = tempfile::tempdir().unwrap();

        let source = LegacyDataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let avro_path = temp_path(&temp_dir, "roundtrip.avro");
        DataFrameWriter::new(&avro_path, FileType::Avro, true, false)
            .execute(source)
            .await
            .unwrap();

        let source2 = LegacyDataFrameReader::new(&avro_path, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *LegacyDataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(count_rows(df3).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_avro_orc_parquet() {
        let select =
            SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
        let temp_dir = tempfile::tempdir().unwrap();

        let source = LegacyDataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            select,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap();
        let orc_path = temp_path(&temp_dir, "roundtrip.orc");
        DataFrameWriter::new(&orc_path, FileType::Orc, true, false)
            .execute(source)
            .await
            .unwrap();

        let source2 = LegacyDataFrameReader::new(&orc_path, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *LegacyDataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(count_rows(df3).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_steps_parquet_tail_to_csv() {
        let read_args = ReadArgs {
            path: "fixtures/table.parquet".to_string(),
            file_type: FileType::Parquet,
            csv_has_header: None,
        };
        let source = DataframeParquetReader { args: read_args }
            .execute(())
            .await
            .unwrap();
        let source = DataframeSelect { select: None }
            .execute(source)
            .await
            .unwrap();
        let source = DataframeTail {
            input_path: "fixtures/table.parquet".to_string(),
            input_file_type: FileType::Parquet,
            n: 2,
        }
        .execute(source)
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        let write_args = WriteArgs {
            path: output.clone(),
            file_type: FileType::Csv,
            sparse: None,
            pretty: None,
        };
        DataframeCsvWriter { args: write_args }
            .execute(Box::new(source))
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_to_record_batch_record_batch_avro_writer() {
        let read_args = ReadArgs {
            path: "fixtures/table.parquet".to_string(),
            file_type: FileType::Parquet,
            csv_has_header: None,
        };
        let source = DataframeParquetReader { args: read_args }
            .execute(())
            .await
            .unwrap();
        let producer = DataframeToRecordBatchProducer::new(source);
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.avro");
        let write_args = WriteArgs {
            path: output.clone(),
            file_type: FileType::Avro,
            sparse: None,
            pretty: None,
        };
        RecordBatchAvroWriter { args: write_args }
            .execute(Box::new(producer))
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }
}
