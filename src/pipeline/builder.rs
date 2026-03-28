//! [`PipelineBuilder`] and planning validation for CLI and REPL pipelines.

use std::path::Path;

use indicatif::ProgressBar;

use super::run::Pipeline;
use crate::Error;
use crate::FileType;
use crate::Result;
use crate::cli::DisplayOutputFormat;
use crate::errors::PipelinePlanningError;
use crate::pipeline::dataframe::DataFramePipeline;
use crate::pipeline::dataframe::DataFrameSink;
use crate::pipeline::record_batch::RecordBatchPipeline;
use crate::pipeline::record_batch::RecordBatchSink;
use crate::pipeline::spec::ColumnSpec;
use crate::pipeline::spec::DisplaySlice;
use crate::pipeline::spec::SelectSpec;
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
            PipelinePlanningError::ConflictingOptions(
                "only one of head(n), tail(n), or sample(n) may be set".to_string(),
            ),
        ));
    }
    Ok(())
}

/// Ensures slice operations (head/tail/sample) are not combined with each other or with
/// `schema` / row-count display, and that at most one of schema vs row-count is set.
fn ensure_display_stage_exclusivity(
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
    schema: bool,
    display_row_count: bool,
) -> Result<()> {
    ensure_at_most_one_of_head_tail_sample(head, tail, sample)?;
    let has_slice = head.is_some() || tail.is_some() || sample.is_some();
    let meta_stages = usize::from(schema) + usize::from(display_row_count);
    if meta_stages > 1 {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::ConflictingOptions(
                "only one of schema(), head(n), tail(n), sample(n), or row count may be set"
                    .to_string(),
            ),
        ));
    }
    if has_slice && meta_stages > 0 {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::ConflictingOptions(
                "schema and row count cannot be combined with head(n), tail(n), or sample(n)"
                    .to_string(),
            ),
        ));
    }
    Ok(())
}

/// Fluent builder for a [`Pipeline`] (file conversion or stdout display: head/tail/sample, schema, or row count).
pub struct PipelineBuilder {
    read: Option<String>,
    select: Option<SelectSpec>,
    head: Option<usize>,
    tail: Option<usize>,
    sample: Option<usize>,
    display_row_count: bool,
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
            display_row_count: false,
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
    /// Display pipeline: print total row count for the read path (and optional [`select`](PipelineBuilder::select_spec)).
    pub fn row_count(&mut self) -> &mut Self {
        self.display_row_count = true;
        self
    }
    /// Display pipeline: print schema for the read path (and optional column selection).
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

        if !input_file_type.supports_pipeline_conversion_input() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }
        if !output_file_type.supports_pipeline_conversion_output() {
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

    fn build_schema_display_pipeline(
        &self,
        input_path: &str,
        select: Option<SelectSpec>,
    ) -> Result<Pipeline> {
        let input_file_type = resolve_file_type(self.input_type_override, input_path)?;
        if !input_file_type.supports_pipeline_display_input() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }

        let output_format = self
            .display_output_format
            .unwrap_or(DisplayOutputFormat::Csv);
        let sparse = self.sparse;
        let input_path = input_path.to_string();
        let csv_has_header = self.csv_has_header;

        if input_file_type == FileType::Orc {
            Ok(Pipeline::RecordBatch(Box::new(RecordBatchPipeline {
                input_path,
                input_file_type,
                select,
                slice: None,
                sparse,
                sink: RecordBatchSink::Schema {
                    output_format,
                    sparse,
                },
            })))
        } else {
            Ok(Pipeline::DataFrame(Box::new(DataFramePipeline {
                input_path,
                input_file_type,
                select,
                slice: None,
                csv_has_header,
                sparse,
                sink: DataFrameSink::Schema {
                    output_format,
                    sparse,
                },
            })))
        }
    }

    fn build_row_count_display_pipeline(
        &self,
        input_path: &str,
        select: Option<SelectSpec>,
    ) -> Result<Pipeline> {
        let input_file_type = resolve_file_type(self.input_type_override, input_path)?;
        if !input_file_type.supports_pipeline_display_input() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }

        let input_path = input_path.to_string();
        let csv_has_header = self.csv_has_header;
        let sparse = self.sparse;

        if input_file_type == FileType::Orc {
            Ok(Pipeline::RecordBatch(Box::new(RecordBatchPipeline {
                input_path,
                input_file_type,
                select,
                slice: None,
                sparse,
                sink: RecordBatchSink::Count,
            })))
        } else {
            Ok(Pipeline::DataFrame(Box::new(DataFramePipeline {
                input_path,
                input_file_type,
                select,
                slice: None,
                csv_has_header,
                sparse,
                sink: DataFrameSink::Count,
            })))
        }
    }

    /// Builds a display pipeline from the builder's configuration.
    fn build_display_pipeline(
        &self,
        input_path: &str,
        select: Option<SelectSpec>,
    ) -> Result<Pipeline> {
        let slice =
            slice_from_head_tail_sample(self.head, self.tail, self.sample).ok_or_else(|| {
                Error::PipelinePlanningError(PipelinePlanningError::MissingRequiredStage(
                    "head(n), tail(n), or sample(n)".to_string(),
                ))
            })?;
        let input_file_type = resolve_file_type(self.input_type_override, input_path)?;
        if !input_file_type.supports_pipeline_display_input() {
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

        if self.write.is_some() && (self.schema || self.display_row_count) {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::ConflictingOptions(
                    "schema and row count are not supported with write(path)".to_string(),
                ),
            ));
        }

        ensure_display_stage_exclusivity(
            self.head,
            self.tail,
            self.sample,
            self.schema,
            self.display_row_count,
        )?;

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
        } else if self.schema {
            self.build_schema_display_pipeline(input_path, select)
        } else if self.display_row_count {
            self.build_row_count_display_pipeline(input_path, select)
        } else {
            self.build_display_pipeline(input_path, select)
        }
    }
}
