use std::io::Cursor;
use std::sync::Arc;

use arrow::json::reader::ReaderBuilder;
use arrow::json::reader::infer_json_schema_from_seekable;
use arrow::json::writer::JsonArray;
use arrow::json::writer::WriterBuilder;
use arrow::record_batch::RecordBatch;

use crate::error::WasmError;
use crate::schema::SchemaField;
use crate::schema::schema_fields_from_arrow;

/// arrow's JSON reader only understands newline-delimited JSON objects, but our own
/// `write_json` (like datu's CLI) emits a single top-level JSON array. Transparently
/// convert an array-wrapped document into NDJSON so round-tripping our own output works;
/// NDJSON input is passed through unchanged.
fn to_ndjson(bytes: &[u8]) -> Result<Vec<u8>, WasmError> {
    let is_array = bytes
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'[');
    if !is_array {
        return Ok(bytes.to_vec());
    }
    let values: Vec<serde_json::Value> = serde_json::from_slice(bytes)?;
    let mut buf = Vec::new();
    for value in values {
        serde_json::to_writer(&mut buf, &value)?;
        buf.push(b'\n');
    }
    Ok(buf)
}

fn infer_schema(ndjson: &[u8]) -> Result<arrow::datatypes::SchemaRef, WasmError> {
    let (schema, _) = infer_json_schema_from_seekable(Cursor::new(ndjson), None)?;
    Ok(Arc::new(schema))
}

/// Extracts schema fields from JSON bytes (via type inference over the whole file).
pub fn json_schema(bytes: &[u8]) -> Result<Vec<SchemaField>, WasmError> {
    let ndjson = to_ndjson(bytes)?;
    let schema = infer_schema(&ndjson)?;
    Ok(schema_fields_from_arrow(&schema))
}

/// Reads all record batches from JSON bytes (newline-delimited objects, or a single
/// top-level JSON array of objects).
pub fn read_json(bytes: &[u8]) -> Result<Vec<RecordBatch>, WasmError> {
    let ndjson = to_ndjson(bytes)?;
    let schema = infer_schema(&ndjson)?;
    let reader = ReaderBuilder::new(schema).build(Cursor::new(&ndjson))?;
    reader
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(WasmError::Arrow)
}

/// Writes record batches as a JSON array (compact or pretty).
pub fn write_json(
    batches: &[RecordBatch],
    sparse: bool,
    pretty: bool,
) -> Result<Vec<u8>, WasmError> {
    let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
    let mut compact = Vec::new();
    let builder = WriterBuilder::new().with_explicit_nulls(!sparse);
    let mut writer = builder.build::<_, JsonArray>(&mut compact);
    writer.write_batches(&batch_refs)?;
    writer.finish()?;

    if pretty {
        let value: serde_json::Value = serde_json::from_slice(&compact)?;
        Ok(serde_json::to_vec_pretty(&value)?)
    } else {
        Ok(compact)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;

    use super::*;

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
    fn test_write_and_read_roundtrip() {
        let batch = make_test_batch();
        let bytes = write_json(&[batch], true, false).unwrap();
        let batches = read_json(&bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_write_pretty_contains_newlines() {
        let batch = make_test_batch();
        let bytes = write_json(&[batch], true, true).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.contains('\n'));
    }

    #[test]
    fn test_schema() {
        let batch = make_test_batch();
        let bytes = write_json(&[batch], true, false).unwrap();
        let fields = json_schema(&bytes).unwrap();
        assert_eq!(fields.len(), 2);
    }
}
