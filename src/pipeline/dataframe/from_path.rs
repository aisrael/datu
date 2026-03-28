//! Load a [`DataFrame`] from a file path (DataFusion formats or ORC via Arrow batches).

use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;

use crate::Error;
use crate::FileType;
use crate::errors::PipelineExecutionError;
use crate::pipeline::orc;
use crate::pipeline::read::ReadResult;
use crate::pipeline::read::read_to_dataframe;

/// Reads a [`DataFrame`] from disk (DataFusion-native formats or ORC via in-memory batches).
pub(crate) async fn read_dataframe_from_path(
    input_path: &str,
    input_file_type: FileType,
    csv_has_header: Option<bool>,
) -> crate::Result<DataFrame> {
    match input_file_type {
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json => {
            let result = read_to_dataframe(input_path, input_file_type, csv_has_header).await?;
            let ReadResult::DataFrame(mut source) = result else {
                unreachable!()
            };
            source
                .df
                .take()
                .ok_or_else(|| Error::from(PipelineExecutionError::DataFrameAlreadyTaken))
        }
        FileType::Orc => {
            let ctx = SessionContext::new();
            let batches = orc::read_orc_all_batches(input_path)?;
            if batches.is_empty() {
                return Err(Error::GenericError(
                    "ORC file is empty or could not be read".to_string(),
                ));
            }
            Ok(ctx.read_batches(batches)?)
        }
        _ => Err(Error::GenericError(
            "Only Parquet, Avro, CSV, JSON, and ORC are supported as input file types".to_string(),
        )),
    }
}
