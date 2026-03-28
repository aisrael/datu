//! [`LegacyDataFrameReader`] step: path → optional select/limit → [`DataFrameSource`](super::source::DataFrameSource).

use async_trait::async_trait;

use super::from_path::read_dataframe_from_path;
use super::source::DataFrameSource;
use super::transform::finalize_dataframe_source;
use crate::FileType;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;

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
    /// Builds a reader for `input_path` with optional projection and row limit.
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
        let df =
            read_dataframe_from_path(&self.input_path, self.input_file_type, self.csv_has_header)
                .await?;
        finalize_dataframe_source(
            df,
            &self.input_path,
            self.input_file_type,
            self.select.as_ref(),
            self.limit,
            None,
        )
        .await
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
