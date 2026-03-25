use std::fs::File;

use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use orc_rust::ArrowReaderBuilder;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelineExecutionError;
use crate::errors::PipelinePlanningError;
use crate::pipeline::dataframe::DataFrameSource;

/// (Legacy) Arguments for reading a file (Avro, CSV, Parquet, ORC).
/// TODO: Remove this once we have a new read API.
#[derive(Default)]
pub struct LegacyReadArgs {
    pub path: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    pub csv_has_header: Option<bool>,
}

/// Arguments for reading a file (Avro, CSV, Parquet, ORC).
pub struct ReadArgs {
    pub path: String,
    pub file_type: FileType,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    pub csv_has_header: Option<bool>,
}

pub enum ReadResult {
    DataFrame(DataFrameSource),
    OrcReaderBuilder(ArrowReaderBuilder<File>),
}

impl std::fmt::Debug for ReadResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadResult::DataFrame(source) => write!(f, "DataFrame({:?})", &source),
            ReadResult::OrcReaderBuilder(builder) => write!(f, "OrcReaderBuilder({:p})", &builder),
        }
    }
}

/// Read a file and return a [ReadResult].
pub async fn read(args: &ReadArgs) -> Result<ReadResult> {
    match args.file_type {
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json => {
            read_to_dataframe(&args.path, args.file_type, args.csv_has_header).await
        }
        FileType::Orc => read_to_record_batches(args),
        FileType::Xlsx | FileType::Yaml => {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(args.file_type.to_string()),
            ));
        }
    }
}

pub async fn read_to_dataframe(
    input_path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<ReadResult> {
    let ctx = SessionContext::new();
    let source = match file_type {
        FileType::Parquet => {
            let df = ctx
                .read_parquet(input_path, ParquetReadOptions::default())
                .await?;
            DataFrameSource::new(ctx, df)
        }
        FileType::Avro => {
            let df = ctx
                .read_avro(input_path, AvroReadOptions::default())
                .await?;
            DataFrameSource::new(ctx, df)
        }
        FileType::Json => {
            let df = ctx
                .read_json(input_path, NdJsonReadOptions::default())
                .await?;
            DataFrameSource::new(ctx, df)
        }
        FileType::Csv => {
            let csv_options = CsvReadOptions::new().has_header(csv_has_header.unwrap_or(true));
            let df = ctx.read_csv(input_path, csv_options).await?;
            DataFrameSource::new(ctx, df)
        }
        _ => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedInputFileType(file_type),
            ));
        }
    };
    Ok(ReadResult::DataFrame(source))
}

pub fn read_to_record_batches(args: &ReadArgs) -> Result<ReadResult> {
    match args.file_type {
        FileType::Orc => {
            let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
            let builder = ArrowReaderBuilder::try_new(file).map_err(Error::OrcError)?;
            Ok(ReadResult::OrcReaderBuilder(builder))
        }
        _ => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedInputFileType(args.file_type),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Source as _;

    #[tokio::test]
    async fn test_read_parquet() {
        let args = ReadArgs {
            path: "fixtures/table.parquet".to_string(),
            file_type: FileType::Parquet,
            csv_has_header: None,
        };
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        source.get().expect("expected DataFrame");
    }

    #[tokio::test]
    async fn test_read_avro() {
        let args = ReadArgs {
            path: "fixtures/userdata5.avro".to_string(),
            file_type: FileType::Avro,
            csv_has_header: None,
        };
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().is_ok());
    }

    #[tokio::test]
    async fn test_read_csv() {
        let args = ReadArgs {
            path: "fixtures/table.csv".to_string(),
            file_type: FileType::Csv,
            csv_has_header: None,
        };
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().is_ok());
    }

    #[tokio::test]
    async fn test_read_json() {
        let args = ReadArgs {
            path: "fixtures/table.json".to_string(),
            file_type: FileType::Json,
            csv_has_header: None,
        };
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().is_ok());
    }

    #[tokio::test]
    async fn test_read_orc() {
        let args = ReadArgs {
            path: "fixtures/userdata.orc".to_string(),
            file_type: FileType::Orc,
            csv_has_header: None,
        };
        let result = read(&args).await;
        println!("result: {:?}", result);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::OrcReaderBuilder(_)));
    }
}
