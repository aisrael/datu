use std::io::Cursor;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use arrow::record_batch::RecordBatch;

use crate::error::WasmError;
use crate::schema::SchemaField;
use crate::schema::schema_fields_from_arrow;

/// Infers a schema from CSV bytes using arrow's native (DataFusion-free) type inference.
/// Note: this can produce slightly different type inference results than datu's CLI,
/// which infers CSV schemas via DataFusion.
fn infer_schema(bytes: &[u8], has_header: bool) -> Result<arrow::datatypes::SchemaRef, WasmError> {
    let format = Format::default().with_header(has_header);
    let (schema, _) = format.infer_schema(&mut Cursor::new(bytes), None)?;
    Ok(Arc::new(schema))
}

/// Extracts schema fields from CSV bytes (via type inference over the whole file).
pub fn csv_schema(bytes: &[u8], has_header: bool) -> Result<Vec<SchemaField>, WasmError> {
    let schema = infer_schema(bytes, has_header)?;
    Ok(schema_fields_from_arrow(&schema))
}

/// Reads all record batches from CSV bytes.
pub fn read_csv(bytes: &[u8], has_header: bool) -> Result<Vec<RecordBatch>, WasmError> {
    let schema = infer_schema(bytes, has_header)?;
    let reader = ReaderBuilder::new(schema)
        .with_header(has_header)
        .build(Cursor::new(bytes))?;
    reader
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(WasmError::Arrow)
}

/// Writes record batches to CSV, returning the encoded bytes.
pub fn write_csv(batches: &[RecordBatch]) -> Result<Vec<u8>, WasmError> {
    let mut buf = Vec::new();
    let mut writer = arrow::csv::Writer::new(&mut buf);
    for batch in batches {
        writer.write(batch)?;
    }
    drop(writer);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_roundtrip() {
        let bytes = std::fs::read("../../fixtures/table.csv").unwrap();
        let batches = read_csv(&bytes, true).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0);

        let csv_bytes = write_csv(&batches).unwrap();
        let reread = read_csv(&csv_bytes, true).unwrap();
        let reread_rows: usize = reread.iter().map(|b| b.num_rows()).sum();
        assert_eq!(reread_rows, total_rows);
    }

    #[test]
    fn test_schema_from_fixture() {
        let bytes = std::fs::read("../../fixtures/table.csv").unwrap();
        let fields = csv_schema(&bytes, true).unwrap();
        assert!(!fields.is_empty());
    }
}
