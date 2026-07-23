use arrow::datatypes::Schema as ArrowSchema;
use serde::Serialize;

/// A schema field with name, data type, and nullability.
#[derive(Clone, Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Extracts schema fields from an Arrow schema (e.g. from record batches read via
/// the CSV/JSON readers, which don't carry Parquet/Avro-specific metadata).
pub fn schema_fields_from_arrow(schema: &ArrowSchema) -> Vec<SchemaField> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}
