use std::io::Write;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use arrow_json::ArrayWriter;
use saphyr::Yaml;
use saphyr::YamlEmitter;

use crate::Error;
use crate::Result;
use crate::cli::DisplayOutputFormat;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;

fn record_batch_to_yaml_rows(batch: &RecordBatch, sparse: bool) -> Vec<Yaml<'static>> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    (0..num_rows)
        .map(|row_idx| {
            let mut map = hashlink::LinkedHashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let array = batch.column(col_idx);
                if sparse && array.is_null(row_idx) {
                    continue;
                }
                let col_name = field.name().clone();
                let value_str =
                    arrow::util::display::array_value_to_string(array.as_ref(), row_idx)
                        .unwrap_or_else(|_| "-".to_string());
                map.insert(
                    Yaml::scalar_from_string(col_name),
                    Yaml::scalar_from_string(value_str),
                );
            }
            Yaml::Mapping(map)
        })
        .collect()
}

/// Write record batches from a reader to the given writer as CSV.
pub fn write_record_batches_as_csv<W>(reader: &mut dyn RecordBatchReader, w: W) -> Result<()>
where
    W: Write,
{
    let mut writer = arrow::csv::Writer::new(w);
    for batch in reader {
        let batch = batch.map_err(Error::ArrowError)?;
        writer.write(&batch).map_err(Error::ArrowError)?;
    }
    Ok(())
}

/// Write record batches from a reader to the given writer as JSON.
pub fn write_record_batches_as_json<W>(reader: &mut dyn RecordBatchReader, w: W) -> Result<()>
where
    W: Write,
{
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(Error::ArrowError)?;
    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
    let mut writer = ArrayWriter::new(w);
    writer
        .write_batches(&batch_refs)
        .map_err(Error::ArrowError)?;
    writer.finish().map_err(Error::ArrowError)?;
    Ok(())
}

/// Write record batches from a reader to the given writer as pretty-formatted JSON.
pub fn write_record_batches_as_json_pretty<W>(
    reader: &mut dyn RecordBatchReader,
    w: W,
) -> Result<()>
where
    W: Write,
{
    let mut buf = Vec::new();
    write_record_batches_as_json(reader, &mut buf)?;
    let value: serde_json::Value = serde_json::from_slice(&buf)
        .map_err(|e| Error::GenericError(format!("Invalid JSON: {e}")))?;
    serde_json::to_writer_pretty(w, &value)
        .map_err(|e| Error::GenericError(format!("Failed to write JSON: {e}")))?;
    Ok(())
}

/// Write record batches from a reader to the given writer as YAML.
pub fn write_record_batches_as_yaml<W>(
    reader: &mut dyn RecordBatchReader,
    mut w: W,
    sparse: bool,
) -> Result<()>
where
    W: Write,
{
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(Error::ArrowError)?;
    let yaml_rows: Vec<Yaml<'static>> = batches
        .iter()
        .flat_map(|batch| record_batch_to_yaml_rows(batch, sparse))
        .collect();
    let doc = Yaml::Sequence(yaml_rows);
    let mut out = String::new();
    let mut emitter = YamlEmitter::new(&mut out);
    emitter
        .dump(&doc)
        .map_err(|e| Error::GenericError(format!("Failed to emit YAML: {e}")))?;
    let to_write = if out.starts_with("---\n") {
        &out[4..]
    } else {
        &out[..]
    };
    write!(w, "{to_write}").map_err(|e| Error::GenericError(format!("Write failed: {e}")))?;
    Ok(())
}

/// Pipeline step that writes record batches to stdout as CSV or JSON.
pub struct DisplayWriterStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub output_format: DisplayOutputFormat,
}

impl Step for DisplayWriterStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = ();

    fn execute(mut self) -> Result<Self::Output> {
        let mut reader = self.prev.get_record_batch_reader()?;
        match self.output_format {
            DisplayOutputFormat::Csv => {
                write_record_batches_as_csv(&mut *reader, std::io::stdout())?;
            }
            DisplayOutputFormat::Json => {
                write_record_batches_as_json(&mut *reader, std::io::stdout())?;
            }
            DisplayOutputFormat::JsonPretty => {
                write_record_batches_as_json_pretty(&mut *reader, std::io::stdout())?;
            }
            DisplayOutputFormat::Yaml => {
                write_record_batches_as_yaml(&mut *reader, std::io::stdout(), true)?;
            }
        }
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

    use super::write_record_batches_as_csv;
    use super::write_record_batches_as_json;
    use super::write_record_batches_as_json_pretty;
    use super::write_record_batches_as_yaml;
    use crate::pipeline::RecordBatchReaderSource;
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

    #[test]
    fn test_write_record_batches_as_csv() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get_record_batch_reader().unwrap();
        let mut out = Vec::new();
        write_record_batches_as_csv(&mut *reader, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("id,name"));
        assert!(s.contains("1,alice"));
        assert!(s.contains("2,bob"));
    }

    #[test]
    fn test_write_record_batches_as_json() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get_record_batch_reader().unwrap();
        let mut out = Vec::new();
        write_record_batches_as_json(&mut *reader, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"id\""));
        assert!(s.contains("\"name\""));
        assert!(s.contains("1"));
        assert!(s.contains("alice"));
        assert!(s.contains("bob"));
    }

    #[test]
    fn test_write_record_batches_as_json_pretty() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get_record_batch_reader().unwrap();
        let mut out = Vec::new();
        write_record_batches_as_json_pretty(&mut *reader, &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"id\""));
        assert!(s.contains("\"name\""));
        assert!(s.contains("1"));
        assert!(s.contains("alice"));
        assert!(s.contains("bob"));
        assert!(s.contains('\n'), "pretty output should contain newlines");
    }

    #[test]
    fn test_write_record_batches_as_yaml() {
        let batch = make_test_batch();
        let mut source = VecRecordBatchReaderSource::new(vec![batch]);
        let mut reader = source.get_record_batch_reader().unwrap();
        let mut out = Vec::new();
        write_record_batches_as_yaml(&mut *reader, &mut out, true).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            !s.starts_with("---\n"),
            "YAML output should not include document start marker"
        );
        assert!(s.contains("id:"));
        assert!(s.contains("name:"));
        assert!(s.contains("1"));
        assert!(s.contains("alice"));
        assert!(s.contains("bob"));
    }
}
