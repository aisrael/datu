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
}
