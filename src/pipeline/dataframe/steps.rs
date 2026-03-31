//! Single-purpose [`Step`]s on [`DataFrameSource`](super::source::DataFrameSource): select, head, tail, sample.

use async_trait::async_trait;

use super::source::DataFrameSource;
use super::transform::apply_select_spec_to_dataframe;
use super::transform::dataframe_apply_head;
use super::transform::dataframe_apply_sample;
use super::transform::dataframe_apply_tail;
use crate::Error;
use crate::FileType;
use crate::errors::PipelineExecutionError;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;

/// Optional column projection on a [`DataFrameSource`].
pub struct DataframeSelect {
    pub select: Option<SelectSpec>,
}

#[async_trait(?Send)]
impl Step for DataframeSelect {
    type Input = DataFrameSource;
    type Output = DataFrameSource;

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let mut df = input
            .df
            .take()
            .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))?;
        if let Some(spec) = self.select.as_ref()
            && !spec.is_empty()
        {
            df = apply_select_spec_to_dataframe(df, spec)?;
        }
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
            .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))?;
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
            .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))?;
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
            .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))?;
        let df = dataframe_apply_sample(df, &self.input_path, self.input_file_type, self.n).await?;
        Ok(DataFrameSource::new(df))
    }
}
