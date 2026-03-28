//! [`DataFrameSource`]: one-shot [`Producer`] of a DataFusion [`DataFrame`].

use async_trait::async_trait;
use datafusion::prelude::DataFrame;

use crate::Error;
use crate::errors::PipelineExecutionError;
use crate::pipeline::Producer;

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
    /// Wraps `df` as a one-shot [`Producer`].
    pub fn new(df: DataFrame) -> Self {
        Self { df: Some(df) }
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataFrameSource {
    async fn get(&mut self) -> crate::Result<Box<DataFrame>> {
        let df = self
            .df
            .take()
            .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))?;
        Ok(Box::new(df))
    }
}
