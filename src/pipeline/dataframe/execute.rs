//! [`DataFramePipeline`] and [`dataframe_pipeline_prepare_source`].

use indicatif::ProgressBar;

use super::from_path::read_dataframe_from_path;
use super::source::DataFrameSource;
use super::transform::finalize_dataframe_source;
use super::writer::write_dataframe_pipeline_output;
use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
use crate::errors::PipelineExecutionError;
use crate::pipeline::DisplaySlice;
use crate::pipeline::FilterSpec;
use crate::pipeline::ProgressVecRecordBatchReader;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::avro;
use crate::pipeline::block_on_pipeline_future;
use crate::pipeline::count_rows;
use crate::pipeline::display::DisplayWriterStep;
use crate::pipeline::schema::get_schema_fields;
use crate::pipeline::schema::print_schema_fields;
use crate::pipeline::schema::schema_fields_from_arrow;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::write_record_batches_from_reader;

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
    Schema {
        output_format: DisplayOutputFormat,
        sparse: bool,
    },
    Count,
}

/// DataFusion-based pipeline: Parquet, Avro, CSV, JSON (not ORC; see [`crate::pipeline::RecordBatchPipeline`]).
pub struct DataFramePipeline {
    pub(crate) input_path: String,
    pub(crate) input_file_type: FileType,
    pub(crate) select: Option<SelectSpec>,
    /// SQL predicate before `select` (`parse_sql_expr` + `filter`).
    pub(crate) filter_before_select: Option<FilterSpec>,
    /// SQL predicate after `select` (post-projection or post-aggregate).
    pub(crate) filter_after_select: Option<FilterSpec>,
    pub(crate) slice: Option<DisplaySlice>,
    pub(crate) csv_has_header: Option<bool>,
    pub(crate) sparse: bool,
    pub(crate) sink: DataFrameSink,
}

impl DataFramePipeline {
    /// Read, optional column select, optional SQL filters before/after select, optional head/tail/sample, then [`DataFrameSink`].
    pub fn execute(&mut self) -> crate::Result<()> {
        let input_path = self.input_path.clone();
        let input_file_type = self.input_file_type;
        let select = self.select.clone();
        let filter_before_select = self.filter_before_select.clone();
        let filter_after_select = self.filter_after_select.clone();
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
            DataFrameSink::Schema {
                output_format,
                sparse: sink_sparse,
            } => DataFrameSink::Schema {
                output_format: *output_format,
                sparse: *sink_sparse,
            },
            DataFrameSink::Count => DataFrameSink::Count,
        };

        let fut = async move {
            match sink {
                DataFrameSink::Schema {
                    output_format,
                    sparse: schema_sparse,
                } => {
                    let use_file_metadata_schema = select.is_none()
                        && filter_before_select.is_none()
                        && filter_after_select.is_none()
                        && matches!(
                            input_file_type,
                            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc
                        );
                    if use_file_metadata_schema {
                        let fields =
                            get_schema_fields(&input_path, input_file_type, csv_has_header)
                                .map_err(|e| Error::GenericError(e.to_string()))?;
                        print_schema_fields(&fields, output_format, schema_sparse)
                            .map_err(|e| Error::GenericError(e.to_string()))?;
                    } else {
                        let mut source = dataframe_pipeline_prepare_source(
                            input_path.clone(),
                            input_file_type,
                            select,
                            filter_before_select.clone(),
                            filter_after_select.clone(),
                            None,
                            csv_has_header,
                        )
                        .await?;
                        let df = source.df.take().ok_or_else(|| {
                            Error::from(PipelineExecutionError::DataFrameAlreadyTaken)
                        })?;
                        let fields = schema_fields_from_arrow(df.schema().as_ref());
                        print_schema_fields(&fields, output_format, schema_sparse)
                            .map_err(|e| Error::GenericError(e.to_string()))?;
                    }
                    Ok::<(), Error>(())
                }
                DataFrameSink::Count => {
                    let total = if select.is_none()
                        && filter_before_select.is_none()
                        && filter_after_select.is_none()
                    {
                        count_rows(&input_path, input_file_type, csv_has_header).await?
                    } else {
                        let mut source = dataframe_pipeline_prepare_source(
                            input_path.clone(),
                            input_file_type,
                            select,
                            filter_before_select.clone(),
                            filter_after_select.clone(),
                            None,
                            csv_has_header,
                        )
                        .await?;
                        let df = source.df.take().ok_or_else(|| {
                            Error::from(PipelineExecutionError::DataFrameAlreadyTaken)
                        })?;
                        df.count().await?
                    };
                    println!("{total}");
                    Ok::<(), Error>(())
                }
                DataFrameSink::Write {
                    output_path,
                    output_file_type,
                    json_pretty,
                    progress,
                } => {
                    let mut source = dataframe_pipeline_prepare_source(
                        input_path,
                        input_file_type,
                        select,
                        filter_before_select.clone(),
                        filter_after_select.clone(),
                        slice,
                        csv_has_header,
                    )
                    .await?;

                    let write_args = WriteArgs {
                        path: output_path.clone(),
                        file_type: output_file_type,
                        sparse: Some(sparse),
                        pretty: Some(json_pretty),
                    };

                    match output_file_type {
                        FileType::Parquet | FileType::Csv | FileType::Json => {
                            write_dataframe_pipeline_output(source, write_args).await?;
                        }
                        FileType::Avro => {
                            avro::DataframeAvroWriter { args: write_args }
                                .execute(Box::new(source))
                                .await?;
                        }
                        FileType::Orc | FileType::Xlsx | FileType::Yaml => {
                            let df = source.df.take().ok_or_else(|| {
                                Error::from(PipelineExecutionError::DataFrameAlreadyTaken)
                            })?;
                            let batches = df.collect().await?;
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
                }
                DataFrameSink::Display {
                    output_format,
                    csv_stdout_headers,
                } => {
                    let mut source = dataframe_pipeline_prepare_source(
                        input_path,
                        input_file_type,
                        select,
                        filter_before_select.clone(),
                        filter_after_select.clone(),
                        slice,
                        csv_has_header,
                    )
                    .await?;
                    let df = source.df.take().ok_or_else(|| {
                        Error::from(PipelineExecutionError::DataFrameAlreadyTaken)
                    })?;
                    let batches = df.collect().await?;
                    let source = Box::new(VecRecordBatchReaderSource::new(batches));
                    let display_step = DisplayWriterStep {
                        output_format,
                        sparse,
                        headers: csv_stdout_headers,
                    };
                    display_step.execute(source).await?;
                    Ok::<(), Error>(())
                }
            }
        };

        block_on_pipeline_future(fut)
    }
}

/// Read into a [`DataFrameSource`], apply optional SQL filters before/after `select`, then optional head/tail/sample.
pub(crate) async fn dataframe_pipeline_prepare_source(
    input_path: String,
    input_file_type: FileType,
    select: Option<SelectSpec>,
    filter_before_select: Option<FilterSpec>,
    filter_after_select: Option<FilterSpec>,
    slice: Option<DisplaySlice>,
    csv_has_header: Option<bool>,
) -> crate::Result<DataFrameSource> {
    let df = match input_file_type {
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json => {
            read_dataframe_from_path(&input_path, input_file_type, csv_has_header).await?
        }
        other => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedInputFileType(other),
            ));
        }
    };
    finalize_dataframe_source(
        df,
        &input_path,
        input_file_type,
        filter_before_select.as_ref().map(FilterSpec::as_str),
        filter_after_select.as_ref().map(FilterSpec::as_str),
        select.as_ref(),
        None,
        slice,
    )
    .await
}
