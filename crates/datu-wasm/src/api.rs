use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use wasm_bindgen::JsError;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::error::WasmError;
use crate::formats;
use crate::metadata::MetadataEntry;
use crate::schema::SchemaField;

#[derive(Clone, Copy)]
enum Format {
    Parquet,
    Avro,
    Csv,
    Json,
}

impl Format {
    fn parse(s: &str) -> Result<Self, WasmError> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" | "parq" => Ok(Format::Parquet),
            "avro" => Ok(Format::Avro),
            "csv" => Ok(Format::Csv),
            "json" => Ok(Format::Json),
            other => Err(WasmError::UnknownFormat(other.to_string())),
        }
    }
}

/// Options controlling CSV/JSON reading and writing. All fields are optional on the JS
/// side; missing fields fall back to the defaults below.
#[derive(serde::Deserialize)]
#[serde(default)]
pub struct ConvertOptions {
    /// Whether the first row of CSV input is a header row.
    pub has_header: bool,
    /// For JSON output: omit null/missing fields.
    pub sparse: bool,
    /// For JSON output: pretty-print with indentation.
    pub pretty: bool,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            sparse: true,
            pretty: false,
        }
    }
}

fn parse_options(options: JsValue) -> Result<ConvertOptions, JsError> {
    if options.is_undefined() || options.is_null() {
        Ok(ConvertOptions::default())
    } else {
        serde_wasm_bindgen::from_value(options).map_err(|e| JsError::new(&e.to_string()))
    }
}

fn schema_any(
    bytes: &[u8],
    format: Format,
    options: &ConvertOptions,
) -> Result<Vec<SchemaField>, WasmError> {
    match format {
        Format::Parquet => formats::parquet::parquet_schema(Bytes::copy_from_slice(bytes)),
        Format::Avro => formats::avro::avro_schema(bytes),
        Format::Csv => formats::csv::csv_schema(bytes, options.has_header),
        Format::Json => formats::json::json_schema(bytes),
    }
}

fn metadata_any(bytes: &[u8], format: Format) -> Result<Vec<MetadataEntry>, WasmError> {
    match format {
        Format::Parquet => formats::parquet::parquet_metadata(Bytes::copy_from_slice(bytes)),
        Format::Avro => formats::avro::avro_metadata(bytes),
        Format::Csv | Format::Json => Ok(Vec::new()),
    }
}

fn read_any(
    bytes: &[u8],
    format: Format,
    options: &ConvertOptions,
) -> Result<(SchemaRef, Vec<RecordBatch>), WasmError> {
    let batches = match format {
        Format::Parquet => formats::parquet::read_parquet(Bytes::copy_from_slice(bytes))?,
        Format::Avro => formats::avro::read_avro(bytes)?,
        Format::Csv => formats::csv::read_csv(bytes, options.has_header)?,
        Format::Json => formats::json::read_json(bytes)?,
    };
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or(WasmError::EmptyInput)?;
    Ok((schema, batches))
}

fn write_any(
    schema: SchemaRef,
    batches: &[RecordBatch],
    format: Format,
    options: &ConvertOptions,
) -> Result<Vec<u8>, WasmError> {
    match format {
        Format::Parquet => formats::parquet::write_parquet(batches, schema),
        Format::Avro => formats::avro::write_avro(batches, schema),
        Format::Csv => formats::csv::write_csv(batches),
        Format::Json => formats::json::write_json(batches, options.sparse, options.pretty),
    }
}

/// Reads the schema of a Parquet, Avro, CSV, or JSON file (as bytes) and returns it as a
/// JS array of `{ name, data_type, nullable }` objects.
#[wasm_bindgen(js_name = inspectSchema)]
pub fn inspect_schema(bytes: &[u8], format: &str, options: JsValue) -> Result<JsValue, JsError> {
    let format = Format::parse(format)?;
    let options = parse_options(options)?;
    let fields = schema_any(bytes, format, &options)?;
    serde_wasm_bindgen::to_value(&fields).map_err(|e| JsError::new(&e.to_string()))
}

/// Reads key/value metadata from a Parquet or Avro file (as bytes) and returns it as a JS
/// array of `{ key, value }` objects. Returns an empty array for CSV/JSON, or when the file
/// has no key/value metadata.
#[wasm_bindgen(js_name = inspectMetadata)]
pub fn inspect_metadata(bytes: &[u8], format: &str) -> Result<JsValue, JsError> {
    let format = Format::parse(format)?;
    let entries = metadata_any(bytes, format)?;
    serde_wasm_bindgen::to_value(&entries).map_err(|e| JsError::new(&e.to_string()))
}

/// Converts file bytes from one format to another. `from_format`/`to_format` are one of
/// `"parquet"`, `"avro"`, `"csv"`, `"json"`.
#[wasm_bindgen]
pub fn convert(
    bytes: &[u8],
    from_format: &str,
    to_format: &str,
    options: JsValue,
) -> Result<Vec<u8>, JsError> {
    let from = Format::parse(from_format)?;
    let to = Format::parse(to_format)?;
    let options = parse_options(options)?;
    let (schema, batches) = read_any(bytes, from, &options)?;
    Ok(write_any(schema, &batches, to, &options)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_bytes() -> Vec<u8> {
        std::fs::read("../../fixtures/table.parquet").unwrap()
    }

    #[test]
    fn test_schema_any_parquet() {
        let bytes = fixture_bytes();
        let options = ConvertOptions::default();
        let fields = schema_any(&bytes, Format::Parquet, &options).unwrap();
        assert_eq!(fields.len(), 6);
    }

    #[test]
    fn test_metadata_any_parquet() {
        let bytes = fixture_bytes();
        let entries = metadata_any(&bytes, Format::Parquet).unwrap();
        assert!(entries.iter().any(|e| e.key == "pandas"));
    }

    #[test]
    fn test_metadata_any_csv_json_empty() {
        assert!(metadata_any(&[], Format::Csv).unwrap().is_empty());
        assert!(metadata_any(&[], Format::Json).unwrap().is_empty());
    }

    #[test]
    fn test_convert_parquet_to_json() {
        let bytes = fixture_bytes();
        let options = ConvertOptions::default();
        let (schema, batches) = read_any(&bytes, Format::Parquet, &options).unwrap();
        let json = write_any(schema, &batches, Format::Json, &options).unwrap();
        let s = String::from_utf8(json).unwrap();
        println!("JSON OUTPUT: {s}");
        assert!(s.starts_with('['));
    }

    #[test]
    fn test_convert_parquet_to_csv() {
        let bytes = fixture_bytes();
        let options = ConvertOptions::default();
        let (schema, batches) = read_any(&bytes, Format::Parquet, &options).unwrap();
        let csv = write_any(schema, &batches, Format::Csv, &options).unwrap();
        let s = String::from_utf8(csv).unwrap();
        println!("CSV OUTPUT: {s}");
        assert!(!s.is_empty());
    }

    #[test]
    fn test_roundtrip_parquet_avro_parquet() {
        let bytes = fixture_bytes();
        let options = ConvertOptions::default();
        let (schema, batches) = read_any(&bytes, Format::Parquet, &options).unwrap();
        let avro_bytes = write_any(schema, &batches, Format::Avro, &options).unwrap();
        println!("AVRO BYTES: {}", avro_bytes.len());
        let (schema2, batches2) = read_any(&avro_bytes, Format::Avro, &options).unwrap();
        let parquet_bytes2 = write_any(schema2, &batches2, Format::Parquet, &options).unwrap();
        println!("ROUNDTRIPPED PARQUET BYTES: {}", parquet_bytes2.len());
        let fields = schema_any(&parquet_bytes2, Format::Parquet, &options).unwrap();
        assert_eq!(fields.len(), 6);
    }
}
