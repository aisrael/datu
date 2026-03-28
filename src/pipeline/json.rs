use std::io::Write;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use arrow_json::writer::JsonArray;
use arrow_json::writer::WriterBuilder;
use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::DataFrame;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::pipeline::DataFrameSource;
use crate::pipeline::Producer;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::dataframe::write_record_batches_from_reader;
use crate::pipeline::read::ReadResult;
use crate::pipeline::read::read_to_dataframe;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteJsonArgs;

/// Writes record batches as a single JSON array (compact or pretty) using Arrow's JSON writer.
pub struct RecordBatchJsonWriter {
    pub sparse: bool,
    pub pretty: bool,
}

impl RecordBatchJsonWriter {
    pub fn new(sparse: bool, pretty: bool) -> Self {
        Self { sparse, pretty }
    }

    pub fn from_write_json_args(args: &WriteJsonArgs) -> Self {
        Self {
            sparse: args.sparse,
            pretty: args.pretty,
        }
    }

    fn write_compact<W: Write>(
        reader: &mut dyn RecordBatchReader,
        w: W,
        sparse: bool,
    ) -> Result<()> {
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
            .map_err(Error::ArrowError)?;
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        let builder = WriterBuilder::new().with_explicit_nulls(!sparse);
        let mut writer = builder.build::<_, JsonArray>(w);
        writer
            .write_batches(&batch_refs)
            .map_err(Error::ArrowError)?;
        writer.finish().map_err(Error::ArrowError)?;
        Ok(())
    }

    pub fn write<W: Write>(&self, reader: &mut dyn RecordBatchReader, w: W) -> Result<()> {
        if self.pretty {
            let mut buf = Vec::new();
            Self::write_compact(reader, &mut buf, self.sparse)?;
            let value: serde_json::Value = serde_json::from_slice(&buf)
                .map_err(|e| Error::GenericError(format!("Invalid JSON: {e}")))?;
            serde_json::to_writer_pretty(w, &value)
                .map_err(|e| Error::GenericError(format!("Failed to write JSON: {e}")))?;
            Ok(())
        } else {
            Self::write_compact(reader, w, self.sparse)
        }
    }

    pub fn write_to_path(&self, reader: &mut dyn RecordBatchReader, path: &str) -> Result<()> {
        let file = std::fs::File::create(path).map_err(Error::IoError)?;
        self.write(reader, file)
    }
}

/// Pipeline step that reads a JSON file and produces a DataFrame.
pub struct DataframeJsonReader {
    pub path: String,
}

#[async_trait(?Send)]
impl Step for DataframeJsonReader {
    type Input = ();
    type Output = DataFrameSource;

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let result = read_to_dataframe(&self.path, FileType::Json, None).await?;
        let ReadResult::DataFrame(source) = result else {
            unreachable!()
        };
        Ok(source)
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataframeJsonReader {
    async fn get(&mut self) -> Result<Box<DataFrame>> {
        let result = read_to_dataframe(&self.path, FileType::Json, None).await?;
        let ReadResult::DataFrame(mut source) = result else {
            unreachable!()
        };
        source.get().await
    }
}

/// Writes a [`DataFrame`] to JSON using DataFusion's native [`DataFrame::write_json`].
pub struct DataframeJsonWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeJsonWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        df.write_json(&self.args.path, DataFrameWriteOptions::default(), None)
            .await?;
        Ok(())
    }
}

/// Writes a [`DataFrame`] to JSON using the display layer ([`write_record_batches_from_reader`])
/// so `--json-pretty` and non-default sparse emission are supported.
pub struct DataframeJsonPrettyWriter {
    pub(crate) args: WriteArgs,
}

#[async_trait(?Send)]
impl Step for DataframeJsonPrettyWriter {
    type Input = Box<dyn Producer<DataFrame>>;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let df = input.get().await?;
        let batches = df.collect().await?;
        let mut reader = VecRecordBatchReader::new(batches);
        let sparse = self.args.sparse.unwrap_or(true);
        let json_pretty = self.args.pretty.unwrap_or(false);
        write_record_batches_from_reader(
            &mut reader,
            &self.args.path,
            FileType::Json,
            sparse,
            json_pretty,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    use super::RecordBatchJsonWriter;
    use crate::pipeline::Producer as _;
    use crate::pipeline::VecRecordBatchReaderSource;

    fn make_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_record_batch_json_writer_compact() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get().await.unwrap();
        let mut out = Vec::new();
        RecordBatchJsonWriter::new(true, false)
            .write(&mut *reader, &mut out)
            .unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"id\""));
        assert!(s.contains("\"name\""));
        assert!(s.contains("1"));
        assert!(s.contains("alice"));
        assert!(s.contains("bob"));
    }

    #[tokio::test]
    async fn test_record_batch_json_writer_pretty() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get().await.unwrap();
        let mut out = Vec::new();
        RecordBatchJsonWriter::new(true, true)
            .write(&mut *reader, &mut out)
            .unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"id\""));
        assert!(s.contains("\"name\""));
        assert!(s.contains("1"));
        assert!(s.contains("alice"));
        assert!(s.contains("bob"));
        assert!(s.contains('\n'), "pretty output should contain newlines");
    }
}
