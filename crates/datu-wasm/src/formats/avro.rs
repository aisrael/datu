use std::io::Cursor;
use std::sync::Arc;

use arrow::compute;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::writer::AvroWriter;

use crate::error::WasmError;
use crate::metadata::MetadataEntry;
use crate::schema::SchemaField;
use crate::schema::schema_fields_from_arrow;

/// Extracts schema fields from Avro file bytes.
pub fn avro_schema(bytes: &[u8]) -> Result<Vec<SchemaField>, WasmError> {
    let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
    Ok(schema_fields_from_arrow(reader.schema().as_ref()))
}

/// Extracts OCF header metadata from Avro file bytes, excluding the `avro.schema` entry
/// (a large duplicate of what the schema-inspection functions already return).
pub fn avro_metadata(bytes: &[u8]) -> Result<Vec<MetadataEntry>, WasmError> {
    let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
    Ok(reader
        .avro_header()
        .metadata()
        .filter(|(key, _)| *key != b"avro.schema")
        .map(|(key, value)| MetadataEntry {
            key: String::from_utf8_lossy(key).into_owned(),
            value: Some(String::from_utf8_lossy(value).into_owned()),
        })
        .collect())
}

/// Reads all record batches from Avro file bytes.
pub fn read_avro(bytes: &[u8]) -> Result<Vec<RecordBatch>, WasmError> {
    let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
    reader
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(WasmError::Arrow)
}

/// Returns true if the schema has any Int16 field (the avro writer does not support Int16).
fn schema_has_int16(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| f.data_type() == &DataType::Int16)
}

/// Builds a schema suitable for the Avro writer: Int16 fields are replaced with Int32.
fn schema_for_avro_writer(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let dt = if f.data_type() == &DataType::Int16 {
                DataType::Int32
            } else {
                f.data_type().clone()
            };
            Field::new(f.name(), dt, f.is_nullable())
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Casts Int16 columns in the batch to Int32 so the batch matches the Avro-compat schema.
fn cast_record_batch_for_avro(
    batch: &RecordBatch,
    compat_schema: &SchemaRef,
) -> arrow::error::Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(batch.num_columns());
    for (i, field) in compat_schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let target_type = field.data_type();
        let cast_col = if col.data_type() == &DataType::Int16 && target_type == &DataType::Int32 {
            compute::cast(col.as_ref(), target_type)?
        } else {
            col.clone()
        };
        columns.push(cast_col);
    }
    RecordBatch::try_new((*compat_schema).clone(), columns)
}

/// Writes record batches to Avro, returning the encoded bytes.
/// Int16 columns are upcast to Int32 since the arrow-avro writer can't write them.
pub fn write_avro(batches: &[RecordBatch], schema: SchemaRef) -> Result<Vec<u8>, WasmError> {
    let (write_schema, batches): (SchemaRef, Vec<RecordBatch>) = if schema_has_int16(&schema) {
        let compat_schema = schema_for_avro_writer(&schema);
        let cast_batches = batches
            .iter()
            .map(|b| cast_record_batch_for_avro(b, &compat_schema))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        (compat_schema, cast_batches)
    } else {
        (schema, batches.to_vec())
    };

    let buf: Vec<u8> = Vec::new();
    let mut writer = AvroWriter::new(buf, (*write_schema).clone())?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner())
}

#[cfg(test)]
mod tests {
    use arrow::array::Int16Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;

    use super::*;

    #[test]
    fn test_write_avro_int16_upcast_to_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int16, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int16Array::from(vec![1_i16, 2, 3]))],
        )
        .unwrap();
        let bytes = write_avro(&[batch], schema).unwrap();
        let batches = read_avro(&bytes).unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
    }

    #[test]
    fn test_schema_and_read_from_fixture() {
        let bytes = std::fs::read("../../fixtures/userdata5.avro").unwrap();
        let fields = avro_schema(&bytes).unwrap();
        assert_eq!(fields.len(), 13);
        let batches = read_avro(&bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1000);
    }

    #[test]
    fn test_metadata_excludes_avro_schema() {
        let bytes = std::fs::read("../../fixtures/userdata5.avro").unwrap();
        let entries = avro_metadata(&bytes).unwrap();
        assert!(entries.iter().all(|e| e.key != "avro.schema"));
    }
}
