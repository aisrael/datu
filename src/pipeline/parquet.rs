use std::fs::File;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use async_trait::async_trait;
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::DataFrame;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::ConvertedType;
use parquet::basic::GzipLevel;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnDescriptor;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::cli::ParquetCompression;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::read::expect_file_type;
use crate::pipeline::record_batch::BatchWriteSink;
use crate::pipeline::record_batch::write_record_batches_with_sink;
use crate::pipeline::schema::SchemaField;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteResult;

impl From<ParquetCompression> for Compression {
    fn from(c: ParquetCompression) -> Self {
        match c {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => {
                Compression::GZIP(GzipLevel::try_new(6).expect("valid gzip level"))
            }
            ParquetCompression::Zstd => {
                Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level"))
            }
            ParquetCompression::Brotli => {
                Compression::BROTLI(BrotliLevel::try_new(1).expect("valid brotli level"))
            }
            ParquetCompression::Lz4 => Compression::LZ4,
            ParquetCompression::Lz4Raw => Compression::LZ4_RAW,
        }
    }
}

impl ParquetCompression {
    /// DataFusion `ParquetOptions.compression` config string equivalent to `Compression::from(self)`.
    fn as_datafusion_config_str(self) -> &'static str {
        match self {
            ParquetCompression::None => "uncompressed",
            ParquetCompression::Snappy => "snappy",
            ParquetCompression::Gzip => "gzip(6)",
            ParquetCompression::Zstd => "zstd(3)",
            ParquetCompression::Brotli => "brotli(1)",
            ParquetCompression::Lz4 => "lz4",
            ParquetCompression::Lz4Raw => "lz4_raw",
        }
    }
}

/// Pipeline step that writes a [`DataFrame`] to Parquet.
pub struct DataframeParquetWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeParquetWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        let mut parquet_options = TableParquetOptions::default();
        parquet_options.global.compression = Some(
            self.args
                .parquet_compression
                .as_datafusion_config_str()
                .to_string(),
        );
        df.write_parquet(
            &self.args.path,
            DataFrameWriteOptions::default(),
            Some(parquet_options),
        )
        .await?;
        Ok(())
    }
}

/// Parquet input for the record-batch pipeline ([`crate::pipeline::build_reader`]): uses the native
/// Arrow Parquet reader so `offset` / `limit` apply without loading the file through DataFusion first.
pub struct RecordBatchParquetReader {
    pub args: ReadArgs,
}

#[async_trait(?Send)]
impl Producer<dyn RecordBatchReader + 'static> for RecordBatchParquetReader {
    async fn get(&mut self) -> Result<Box<dyn RecordBatchReader + 'static>> {
        read_parquet(&self.args)
            .map(|reader| Box::new(reader) as Box<dyn RecordBatchReader + 'static>)
    }
}

/// Internal representation of a schema column from Parquet metadata.
struct SchemaOutput {
    column_name: String,
    data_type: String,
    converted_type: Option<ConvertedType>,
    nullable: bool,
}

impl SchemaOutput {
    fn to_schema_field(&self) -> SchemaField {
        SchemaField {
            name: self.column_name.clone(),
            data_type: self.data_type.clone(),
            converted_type: self.converted_type.as_ref().map(|ct| format!("{ct:?}")),
            nullable: self.nullable,
        }
    }
}

fn column_to_schema_output(column: &Arc<ColumnDescriptor>) -> SchemaOutput {
    let path = column.path();
    let physical_type = column.physical_type();
    let logical_type = column.logical_type_ref();
    let converted_type = column.converted_type();

    let column_name = path.parts().join(".");

    let data_type = if let Some(logical) = logical_type {
        format!("{:?}", logical)
    } else {
        format!("{}", physical_type)
    };

    let converted_type = if matches!(converted_type, ConvertedType::NONE) {
        None
    } else {
        Some(converted_type)
    };

    let nullable = column.max_def_level() > 0;

    SchemaOutput {
        column_name,
        data_type,
        converted_type,
        nullable,
    }
}

/// Extracts schema fields from a Parquet file path (metadata only).
pub fn get_schema_fields_parquet(path: &str) -> eyre::Result<Vec<SchemaField>> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;

    let file_metadata = metadata.file_metadata();
    let schema_descr = file_metadata.schema_descr();

    let columns: Vec<SchemaOutput> = schema_descr
        .columns()
        .iter()
        .map(column_to_schema_output)
        .collect();

    Ok(columns.iter().map(SchemaOutput::to_schema_field).collect())
}

/// Read a parquet file and return a RecordBatchReader.
///
/// `with_offset` / `with_limit` on the Arrow Parquet reader apply the same global-row semantics as
/// [`crate::pipeline::record_batch::apply_offset_limit`] (skip `offset` rows, then yield at most
/// `limit` rows).
pub fn read_parquet(args: &ReadArgs) -> Result<ParquetRecordBatchReader> {
    expect_file_type(args, FileType::Parquet)?;
    let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;

    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(Error::ParquetError)?;
    if let Some(offset) = args.offset {
        builder = builder.with_offset(offset);
    }
    if let Some(limit) = args.limit {
        builder = builder.with_limit(limit);
    }
    builder.build().map_err(Error::ParquetError)
}

/// Pipeline step that writes record batches to a Parquet file.
pub struct RecordBatchParquetWriter {
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

#[async_trait(?Send)]
impl Step for RecordBatchParquetWriter {
    type Input = ();
    type Output = WriteResult;

    async fn execute(mut self, _input: Self::Input) -> Result<Self::Output> {
        let mut reader = self.source.get().await?;
        let compression = self.args.parquet_compression;
        write_record_batches_with_sink(&self.args.path, &mut *reader, move |p, s| {
            ParquetSink::new(p, s, compression)
        })?;
        Ok(WriteResult)
    }
}

pub(crate) struct ParquetSink {
    writer: ArrowWriter<std::fs::File>,
}

impl ParquetSink {
    pub(crate) fn new(
        path: &str,
        schema: arrow::datatypes::SchemaRef,
        compression: ParquetCompression,
    ) -> Result<Self> {
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        let props = WriterProperties::builder()
            .set_compression(compression.into())
            .build();
        let writer =
            ArrowWriter::try_new(file, schema, Some(props)).map_err(Error::ParquetError)?;
        Ok(Self { writer })
    }
}

impl BatchWriteSink for ParquetSink {
    fn write_batch(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(Error::ParquetError)
    }

    fn finish(self) -> Result<()> {
        self.writer.close().map_err(Error::ParquetError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::FileType;
    use crate::pipeline::DataframeParquetReader;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_parque_step_dataframe() {
        let args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let mut step = DataframeParquetReader { args };
        let df = step.get().await.expect("Failed to read Parquet file");
        assert_eq!(
            df.count().await.expect("Failed to count rows"),
            3,
            "Expected 3 rows"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_and_write_parquet_steps() {
        let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let read_step = DataframeParquetReader { args: read_args };

        let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
        let args = WriteArgs {
            path: tempfile
                .path()
                .to_str()
                .expect("Failed to get path")
                .to_string(),
            file_type: FileType::Parquet,
            sparse: None,
            pretty: None,
            avro_compression: crate::cli::AvroCompression::None,
            parquet_compression: ParquetCompression::None,
        };
        let step = DataframeParquetWriter { args };
        step.execute(Box::new(read_step))
            .await
            .expect("Failed to write Parquet file");
        assert!(tempfile.path().exists());

        let schema =
            get_schema_fields_parquet(tempfile.path().to_str().expect("Failed to get path"))
                .expect("Failed to get schema fields");
        assert_eq!(schema.len(), 6, "Expected 6 columns");
        assert_eq!(schema[0].name, "one", "Expected first column name is 'one'");
        assert_eq!(schema[0].data_type, "DOUBLE", "Expected DOUBLE data type");
        assert!(schema[0].nullable, "Expected nullable column");
        assert_eq!(
            schema[1].name, "two",
            "Expected second column name is 'two'"
        );
        assert_eq!(schema[1].data_type, "String", "Expected String data type");
        assert!(schema[1].nullable, "Expected nullable column");
    }

    fn column_compression(path: &std::path::Path) -> Compression {
        let file = std::fs::File::open(path).expect("Failed to open file");
        let reader = parquet::file::reader::SerializedFileReader::new(file)
            .expect("Expected file to be valid Parquet");
        let metadata = parquet::file::reader::FileReader::metadata(&reader);
        metadata.row_group(0).column(0).compression()
    }

    async fn write_dataframe_parquet_with_compression(
        compression: ParquetCompression,
    ) -> NamedTempFile {
        let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let read_step = DataframeParquetReader { args: read_args };
        let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
        let args = WriteArgs {
            path: tempfile
                .path()
                .to_str()
                .expect("Failed to get path")
                .to_string(),
            file_type: FileType::Parquet,
            sparse: None,
            pretty: None,
            avro_compression: crate::cli::AvroCompression::None,
            parquet_compression: compression,
        };
        DataframeParquetWriter { args }
            .execute(Box::new(read_step))
            .await
            .expect("Failed to write Parquet file");
        tempfile
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_parquet_writer_with_snappy_compression() {
        let tempfile = write_dataframe_parquet_with_compression(ParquetCompression::Snappy).await;
        assert_eq!(column_compression(tempfile.path()), Compression::SNAPPY);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_parquet_writer_with_gzip_compression() {
        // Parquet's on-disk column metadata stores only the codec identifier, not the level used
        // to write it (the level is a write-time-only tuning parameter), so only the variant is
        // checked here.
        let tempfile = write_dataframe_parquet_with_compression(ParquetCompression::Gzip).await;
        assert!(matches!(
            column_compression(tempfile.path()),
            Compression::GZIP(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataframe_parquet_writer_with_no_compression() {
        let tempfile = write_dataframe_parquet_with_compression(ParquetCompression::None).await;
        assert_eq!(
            column_compression(tempfile.path()),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn test_record_batch_parquet_sink_with_zstd_compression() {
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "x",
            arrow::datatypes::DataType::Int32,
            false,
        )]);
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("out.parquet");
        let mut reader = crate::pipeline::VecRecordBatchReader::new(vec![batch]);
        let mut sink = ParquetSink::new(
            path.to_str().unwrap(),
            reader.schema(),
            ParquetCompression::Zstd,
        )
        .unwrap();
        for batch in &mut reader {
            sink.write_batch(&batch.unwrap()).unwrap();
        }
        sink.finish().unwrap();
        // See the comment on test_dataframe_parquet_writer_with_gzip_compression: only the codec
        // identifier round-trips through the file, not the write-time level.
        assert!(matches!(column_compression(&path), Compression::ZSTD(_)));
    }

    #[test]
    fn test_read_parquet() {
        let args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let mut reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let batch = reader
            .next()
            .expect("None")
            .map_err(Error::ArrowError)
            .expect("Unable to read batch");
        assert_eq!(batch.num_rows(), 3, "Expected 3 rows");
    }

    #[test]
    fn test_read_parquet_with_limit() {
        let mut args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        args.limit = Some(1);
        let mut reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let batch = reader
            .next()
            .expect("None")
            .map_err(Error::ArrowError)
            .expect("Unable to read batch");
        assert_eq!(batch.num_rows(), 1, "Expected only 1 row");
    }

    #[test]
    fn test_read_parquet_with_offset() {
        let mut args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        args.offset = Some(1);
        let reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let total: usize = reader
            .map(|b| b.map_err(Error::ArrowError).expect("batch").num_rows())
            .sum();
        assert_eq!(total, 2, "Expected 2 rows after skipping 1");
    }

    #[test]
    fn test_read_parquet_with_offset_and_limit() {
        let mut args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        args.offset = Some(1);
        args.limit = Some(1);
        let mut reader =
            read_parquet(&args).expect("read_parquet failed to return a ParquetRecordBatchReader");
        let batch = reader
            .next()
            .expect("None")
            .map_err(Error::ArrowError)
            .expect("Unable to read batch");
        assert_eq!(
            batch.num_rows(),
            1,
            "Expected 1 row after offset 1 and limit 1"
        );
        assert!(reader.next().is_none());
    }
}
