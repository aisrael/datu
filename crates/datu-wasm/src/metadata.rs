use serde::Serialize;

/// A single key/value metadata entry (e.g. a Parquet `key_value_metadata` pair, or an Avro
/// OCF header metadata entry).
#[derive(Clone, Serialize)]
pub struct MetadataEntry {
    pub key: String,
    pub value: Option<String>,
}
