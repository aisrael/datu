use arrow::array::RecordBatchReader;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::pipeline::DataFrameSource;
use crate::pipeline::avro;
use crate::pipeline::dataframe::write_dataframe_pipeline_output;
use crate::pipeline::display;
use crate::pipeline::json::RecordBatchJsonWriter;
use crate::pipeline::orc;
use crate::pipeline::parquet::ParquetSink;
use crate::pipeline::record_batch::write_record_batches_with_sink;
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

/// Writes record batches from a reader to an output file. Single dispatch for DataFrame batch
/// export, ORC record-batch pipeline writes, and JSON pretty/sparse paths.
pub fn write_record_batches_from_reader(
    reader: &mut dyn RecordBatchReader,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> Result<()> {
    if output_file_type != FileType::Json && json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }

    match output_file_type {
        FileType::Parquet => write_record_batches_with_sink(output_path, reader, ParquetSink::new)?,
        FileType::Csv => crate::pipeline::csv::write_record_batches(output_path, reader)?,
        FileType::Json => {
            RecordBatchJsonWriter::new(sparse, json_pretty).write_to_path(reader, output_path)?;
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

/// Writes a [`DataFrame`] to Parquet, CSV, or JSON using [`write_dataframe_pipeline_output`].
pub async fn write_dataframe(source: DataFrameSource, args: WriteArgs) -> Result<()> {
    write_dataframe_pipeline_output(source, args).await
}
