use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::ColumnDescriptor;

use crate::error::WasmError;
use crate::metadata::MetadataEntry;
use crate::schema::SchemaField;

fn column_to_schema_field(column: &Arc<ColumnDescriptor>) -> SchemaField {
    let physical_type = column.physical_type();
    let logical_type = column.logical_type_ref();
    let converted_type = column.converted_type();

    let name = column.path().parts().join(".");

    let data_type = if let Some(logical) = logical_type {
        format!("{:?}", logical)
    } else if !matches!(converted_type, ConvertedType::NONE) {
        format!("{:?}", converted_type)
    } else {
        format!("{}", physical_type)
    };

    let nullable = column.max_def_level() > 0;

    SchemaField {
        name,
        data_type,
        nullable,
    }
}

/// Extracts schema fields from Parquet file bytes (metadata only, no data read).
pub fn parquet_schema(bytes: Bytes) -> Result<Vec<SchemaField>, WasmError> {
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    let schema_descr = metadata.file_metadata().schema_descr();
    Ok(schema_descr
        .columns()
        .iter()
        .map(column_to_schema_field)
        .collect())
}

/// Extracts key/value metadata from Parquet file bytes (metadata only, no data read).
pub fn parquet_metadata(bytes: Bytes) -> Result<Vec<MetadataEntry>, WasmError> {
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    Ok(metadata
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| MetadataEntry {
                    key: kv.key.clone(),
                    value: kv.value.clone(),
                })
                .collect()
        })
        .unwrap_or_default())
}

/// Reads all record batches from Parquet file bytes.
pub fn read_parquet(bytes: Bytes) -> Result<Vec<RecordBatch>, WasmError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
    reader
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(WasmError::Arrow)
}

/// Writes record batches to Parquet, returning the encoded bytes.
pub fn write_parquet(batches: &[RecordBatch], schema: SchemaRef) -> Result<Vec<u8>, WasmError> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;

    use super::*;

    fn make_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        let batch = make_test_batch();
        let bytes = write_parquet(&[batch.clone()], batch.schema()).unwrap();
        let read_back = read_parquet(Bytes::from(bytes)).unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].num_rows(), 3);
    }

    #[test]
    fn test_schema_from_fixture() {
        let bytes = std::fs::read("../../fixtures/table.parquet").unwrap();
        let fields = parquet_schema(Bytes::from(bytes)).unwrap();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].name, "one");
        assert!(fields[0].nullable);
    }

    #[test]
    fn test_read_fixture() {
        let bytes = std::fs::read("../../fixtures/table.parquet").unwrap();
        let batches = read_parquet(Bytes::from(bytes)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_metadata_from_fixture() {
        let bytes = std::fs::read("../../fixtures/table.parquet").unwrap();
        let entries = parquet_metadata(Bytes::from(bytes)).unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert!(keys.contains(&"pandas"));
        assert!(keys.contains(&"ARROW:schema"));
    }

    #[test]
    fn test_metadata_includes_arrow_schema_for_written_file() {
        // ArrowWriter embeds an "ARROW:schema" key by default (round-trip support), even
        // when the caller supplies no custom key/value metadata.
        let batch = make_test_batch();
        let bytes = write_parquet(std::slice::from_ref(&batch), batch.schema()).unwrap();
        let entries = parquet_metadata(Bytes::from(bytes)).unwrap();
        assert!(entries.iter().any(|e| e.key == "ARROW:schema"));
    }
}
