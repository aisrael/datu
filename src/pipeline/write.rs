use datafusion::dataframe::DataFrameWriteOptions;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelineExecutionError;
use crate::pipeline::DataFrameSource;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::avro;
use crate::pipeline::display::write_record_batches_as_yaml;
use crate::pipeline::orc;
use crate::pipeline::xlsx;

/// Arguments for writing a file (CSV, Avro, Parquet, ORC, XLSX).
pub struct WriteArgs {
    pub path: String,
    pub file_type: FileType,
    pub sparse: Option<bool>,
    pub pretty: Option<bool>,
}

/// Arguments for writing a JSON file.
pub struct WriteJsonArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
    /// When true, format output with indentation and newlines.
    pub pretty: bool,
}

/// Arguments for writing a YAML file.
pub struct WriteYamlArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
}

/// Marker returned by record-batch write pipeline steps after a successful write.
pub struct WriteResult;

/// Writes a [`DataFrame`] using DataFusion native writers (Parquet, CSV, JSON only).
pub async fn write_dataframe(mut source: DataFrameSource, args: WriteArgs) -> Result<()> {
    let write_opts = DataFrameWriteOptions::new();
    let df = source
        .df
        .take()
        .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
    match args.file_type {
        FileType::Parquet => {
            let _ = df.write_parquet(&args.path, write_opts, None).await?;
        }
        FileType::Csv => {
            let _ = df.write_csv(&args.path, write_opts, None).await?;
        }
        FileType::Json => {
            df.write_json(&args.path, write_opts, None).await?;
        }
        FileType::Avro | FileType::Orc | FileType::Xlsx | FileType::Yaml => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedOutputFileType(args.file_type),
            ));
        }
    }
    Ok(())
}

/// Writes record batches for formats that need a record-batch path (Avro, ORC, XLSX, YAML).
pub async fn write_record_batches(
    mut source: RecordBatchReaderSource,
    args: WriteArgs,
) -> Result<()> {
    let mut reader = source.get().await?;
    let path = args.path.as_str();
    match args.file_type {
        FileType::Avro => {
            avro::write_record_batches(path, &mut reader)?;
        }
        FileType::Orc => {
            orc::write_record_batches(path, &mut reader)?;
        }
        FileType::Xlsx => {
            xlsx::write_record_batch_to_xlsx(path, &mut reader)?;
        }
        FileType::Yaml => {
            let file = std::fs::File::create(path).map_err(Error::IoError)?;
            let sparse = args.sparse.unwrap_or(false);
            write_record_batches_as_yaml(&mut *reader, file, sparse)?;
        }
        _ => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedOutputFileType(args.file_type),
            ));
        }
    }
    Ok(())
}
