use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use futures::StreamExt;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
use crate::errors::PipelineExecutionError;
use crate::pipeline::ConversionPipeline;
use crate::pipeline::DisplayPipeline;
use crate::pipeline::DisplaySlice;
use crate::pipeline::ProgressVecRecordBatchReader;
use crate::pipeline::SelectSpec;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::avro;
use crate::pipeline::display;
use crate::pipeline::display::DisplayWriterStep;
use crate::pipeline::orc;
use crate::pipeline::parquet;
use crate::pipeline::reservoir_sample_from_reader;
use crate::pipeline::sample_from_reader;
use crate::pipeline::tail_batches;
use crate::pipeline::xlsx;

/// A source that yields a DataFusion DataFrame, implementing `Source<DataFrame>`.
pub struct DataFrameSource {
    session_context: SessionContext,
    df: Option<datafusion::dataframe::DataFrame>,
}

impl std::fmt::Debug for DataFrameSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataFrameSource {{ session_context: {:p}, df: {:?} }}",
            &self.session_context as *const SessionContext, self.df
        )
    }
}

impl DataFrameSource {
    pub fn new(session_context: SessionContext, df: datafusion::dataframe::DataFrame) -> Self {
        Self {
            session_context,
            df: Some(df),
        }
    }
}

impl Source<datafusion::dataframe::DataFrame> for DataFrameSource {
    fn get(&mut self) -> crate::Result<Box<datafusion::dataframe::DataFrame>> {
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
        let df = input.get()?;

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
pub struct DataFrameReader {
    input_path: String,
    input_file_type: FileType,
    select: Option<SelectSpec>,
    limit: Option<usize>,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    csv_has_header: Option<bool>,
}

impl DataFrameReader {
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

        Ok(DataFrameSource::new(ctx, df))
    }
}

#[async_trait(?Send)]
impl Step for DataFrameReader {
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
pub struct DataFrameToBatchReader {
    schema: Arc<arrow::datatypes::Schema>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl DataFrameToBatchReader {
    pub async fn try_new(mut source: DataFrameSource) -> crate::Result<Self> {
        let df = *source.get()?;
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

impl Iterator for DataFrameToBatchReader {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle.clone();
        tokio::task::block_in_place(|| handle.block_on(self.stream.next()))
            .map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for DataFrameToBatchReader {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Reads via DataFusion (`SessionContext`), optional column select, prints to stdout.
/// Used for Parquet, CSV, JSON, and Avro. ORC uses [`crate::pipeline::RecordBatchDisplayPipeline`].
pub struct DataFrameDisplayPipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    pub(crate) slice: DisplaySlice,
    pub(crate) csv_has_header: Option<bool>,
    pub(crate) output_format: DisplayOutputFormat,
    pub(crate) sparse: bool,
    pub(crate) csv_stdout_headers: bool,
}

impl DisplayPipeline for DataFrameDisplayPipeline {
    fn execute(&mut self) -> crate::Result<()> {
        let input_path = self.input_path.clone();
        let input_file_type = self.input_file_type;
        let select = self.select.clone();
        let slice = self.slice;
        let csv_has_header = self.csv_has_header;
        let output_format = self.output_format;
        let sparse = self.sparse;
        let csv_stdout_headers = self.csv_stdout_headers;

        let fut = async move {
            let ctx = SessionContext::new();
            let has_header = csv_has_header.unwrap_or(true);

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
                    .read_csv(&input_path, CsvReadOptions::new().has_header(has_header))
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Json => ctx
                    .read_json(&input_path, NdJsonReadOptions::default())
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                other => {
                    return Err(Error::GenericError(format!(
                        "DataFrameDisplayPipeline does not support input type: {other}"
                    )));
                }
            };

            if let Some(spec) = &select
                && !spec.is_empty()
            {
                let schema = df.schema();
                let resolved = spec.resolve_names(schema.as_ref())?;
                let col_refs: Vec<&str> = resolved.iter().map(String::as_str).collect();
                df = df
                    .select_columns(&col_refs)
                    .map_err(|e| Error::GenericError(e.to_string()))?;
            }

            let batches = match slice {
                DisplaySlice::Head(n) => {
                    let df = df
                        .limit(0, Some(n))
                        .map_err(|e| Error::GenericError(e.to_string()))?;
                    df.collect()
                        .await
                        .map_err(|e| Error::GenericError(e.to_string()))?
                }
                DisplaySlice::Tail(tail_n) => match input_file_type {
                    FileType::Parquet => {
                        let total_rows =
                            crate::get_total_rows_result(&input_path, FileType::Parquet)?;
                        let number = tail_n.min(total_rows);
                        let skip = total_rows.saturating_sub(number);
                        let df = df
                            .limit(skip, Some(number))
                            .map_err(|e| Error::GenericError(e.to_string()))?;
                        df.collect()
                            .await
                            .map_err(|e| Error::GenericError(e.to_string()))?
                    }
                    FileType::Csv | FileType::Json | FileType::Avro => {
                        let all = df
                            .collect()
                            .await
                            .map_err(|e| Error::GenericError(e.to_string()))?;
                        tail_batches(all, tail_n)
                    }
                    other => {
                        return Err(Error::GenericError(format!(
                            "DataFrameDisplayPipeline tail unsupported for: {other}"
                        )));
                    }
                },
                DisplaySlice::Sample(n) => match input_file_type {
                    FileType::Parquet => {
                        let total_rows =
                            crate::get_total_rows_result(&input_path, FileType::Parquet)?;
                        let source = DataFrameSource::new(ctx, df);
                        let batch_reader = DataFrameToBatchReader::try_new(source).await?;
                        sample_from_reader(Box::new(batch_reader), total_rows, n)
                    }
                    FileType::Avro | FileType::Csv | FileType::Json => {
                        let source = DataFrameSource::new(ctx, df);
                        let batch_reader = DataFrameToBatchReader::try_new(source).await?;
                        reservoir_sample_from_reader(Box::new(batch_reader), n)
                    }
                    other => {
                        return Err(Error::GenericError(format!(
                            "DataFrameDisplayPipeline sample unsupported for: {other}"
                        )));
                    }
                },
            };

            let source = Box::new(VecRecordBatchReaderSource::new(batches));
            let display_step = DisplayWriterStep {
                output_format,
                sparse,
                headers: csv_stdout_headers,
            };
            display_step.execute(source).await
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

pub struct DataFramePipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    pub(crate) head: Option<usize>,
    pub(crate) output_path: String,
    pub(crate) output_file_type: FileType,
    pub(crate) csv_has_header: Option<bool>,
    pub(crate) sparse: bool,
    pub(crate) json_pretty: bool,
    pub(crate) progress: Option<ProgressBar>,
}

impl ConversionPipeline for DataFramePipeline {
    fn execute(&mut self) -> crate::Result<()> {
        let input_path = self.input_path.clone();
        let input_file_type = self.input_file_type;
        let select = self.select.clone();
        let head = self.head;
        let output_path = self.output_path.clone();
        let output_file_type = self.output_file_type;
        let csv_has_header = self.csv_has_header;
        let sparse = self.sparse;
        let json_pretty = self.json_pretty;
        let progress = self.progress.clone();

        let fut = async move {
            let ctx = SessionContext::new();
            let has_header = csv_has_header.unwrap_or(true);

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
                    .read_csv(&input_path, CsvReadOptions::new().has_header(has_header))
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Json => ctx
                    .read_json(&input_path, NdJsonReadOptions::default())
                    .await
                    .map_err(|e| Error::GenericError(e.to_string()))?,
                FileType::Orc => {
                    let batches = orc::read_orc_all_batches(&input_path)?;
                    if batches.is_empty() {
                        return Err(Error::GenericError(
                            "ORC file is empty or could not be read".to_string(),
                        ));
                    }
                    ctx.read_batches(batches)
                        .map_err(|e| Error::GenericError(e.to_string()))?
                }
                other => {
                    return Err(Error::PipelineExecutionError(
                        PipelineExecutionError::UnsupportedInputFileType(other),
                    ));
                }
            };

            if let Some(spec) = &select
                && !spec.is_empty()
            {
                let schema = df.schema();
                let resolved = spec.resolve_names(schema.as_ref())?;
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
            match output_file_type {
                FileType::Parquet => {
                    df.write_parquet(&output_path, write_opts, None)
                        .await
                        .map_err(|e| Error::GenericError(e.to_string()))?;
                }
                FileType::Csv => {
                    df.write_csv(&output_path, write_opts, None)
                        .await
                        .map_err(|e| Error::GenericError(e.to_string()))?;
                }
                FileType::Json
                | FileType::Avro
                | FileType::Orc
                | FileType::Xlsx
                | FileType::Yaml => {
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::FileType;
    use crate::pipeline::SelectSpec;
    use crate::pipeline::Source;
    use crate::pipeline::Step;
    use crate::pipeline::dataframe::DataFrameReader;
    use crate::pipeline::dataframe::DataFrameWriter;
    use crate::pipeline::read_to_batches;
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
        let df = *DataFrameReader::new(
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
        .unwrap();
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select() {
        let select = SelectSpec::from_cli_args(&Some(vec!["one".to_string(), "two".to_string()]));
        let df = *DataFrameReader::new(
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
        .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["one", "two"]);
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_limit() {
        let df = *DataFrameReader::new(
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
        .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select_and_limit() {
        let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string()]));
        let df = *DataFrameReader::new(
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
        .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["two"]);
        assert_eq!(count_rows(df).await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_avro() {
        let df = *DataFrameReader::new(
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
        .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_orc() {
        let df = *DataFrameReader::new("fixtures/userdata.orc", FileType::Orc, None, Some(5), None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_csv() {
        let df = *DataFrameReader::new("fixtures/table.csv", FileType::Csv, None, Some(2), None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_unsupported_type() {
        let result =
            DataFrameReader::new("fixtures/table.parquet", FileType::Xlsx, None, None, None)
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
        let source = DataFrameReader::new(
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

        let df2 = *DataFrameReader::new(&output, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_csv() {
        let source = DataFrameReader::new(
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
        let source = DataFrameReader::new(
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
        let source = DataFrameReader::new(
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
        let source = DataFrameReader::new(
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
        let source = DataFrameReader::new(
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

        let df2 = *DataFrameReader::new(&output, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_orc() {
        let select =
            SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
        let source = DataFrameReader::new(
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

        let df2 = *DataFrameReader::new(&output, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_xlsx() {
        let source = DataFrameReader::new(
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

        let source = DataFrameReader::new(
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

        let source2 = DataFrameReader::new(&avro_path, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *DataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_avro_orc_parquet() {
        let select =
            SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
        let temp_dir = tempfile::tempdir().unwrap();

        let source = DataFrameReader::new(
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

        let source2 = DataFrameReader::new(&orc_path, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *DataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 5);
    }
}
