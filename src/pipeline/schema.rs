//! Schema extraction and display for datu.
//!
//! Provides `get_schema_fields` to extract schema from files (used by CLI and REPL)
//! and `schema_fields_from_arrow` for schema from in-memory batches (used by REPL).

use std::fs::File;

use arrow::array::RecordBatchReader;
use arrow::datatypes::Schema as ArrowSchema;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;
use eyre::Result;
use eyre::bail;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use saphyr::Scalar;
use saphyr::Yaml;
use saphyr::YamlEmitter;
use serde::Serialize;

use crate::FileType;
use crate::cli::DisplayOutputFormat;

/// A schema field with name, data type, converted type (if any), and nullability.
#[derive(Clone, Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub converted_type: Option<String>,
    pub nullable: bool,
}

impl SchemaField {
    /// Converts this field to a YAML mapping entry; when `sparse` is true, omits null values.
    fn to_yaml_mapping(&self, sparse: bool) -> Yaml<'static> {
        let mut map = hashlink::LinkedHashMap::new();
        map.insert(
            Yaml::scalar_from_string("name".to_string()),
            Yaml::scalar_from_string(self.name.clone()),
        );
        map.insert(
            Yaml::scalar_from_string("data_type".to_string()),
            Yaml::scalar_from_string(self.data_type.clone()),
        );
        match &self.converted_type {
            Some(ct) => {
                map.insert(
                    Yaml::scalar_from_string("converted_type".to_string()),
                    Yaml::scalar_from_string(ct.clone()),
                );
            }
            None => {
                if !sparse {
                    map.insert(
                        Yaml::scalar_from_string("converted_type".to_string()),
                        Yaml::Value(Scalar::Null),
                    );
                }
            }
        }
        map.insert(
            Yaml::scalar_from_string("nullable".to_string()),
            Yaml::Value(Scalar::Boolean(self.nullable)),
        );
        Yaml::Mapping(map)
    }
}

/// Full schema field used when sparse=false; all optional fields are always serialized.
#[derive(Clone, Serialize)]
struct SchemaFieldFull {
    name: String,
    data_type: String,
    converted_type: Option<String>,
    nullable: bool,
}

impl From<&SchemaField> for SchemaFieldFull {
    fn from(f: &SchemaField) -> Self {
        SchemaFieldFull {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            converted_type: f.converted_type.clone(),
            nullable: f.nullable,
        }
    }
}

/// Extracts schema fields from an Arrow schema (e.g. from in-memory record batches).
/// Used by the REPL when displaying schema of loaded data.
pub fn schema_fields_from_arrow(schema: &ArrowSchema) -> Vec<SchemaField> {
    schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            converted_type: None,
            nullable: f.is_nullable(),
        })
        .collect()
}

/// Extracts schema fields from a file path.
/// Used by the CLI schema command.
pub fn get_schema_fields(
    path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<Vec<SchemaField>> {
    match file_type {
        FileType::Parquet => crate::pipeline::parquet::get_schema_fields_parquet(path),
        FileType::Avro => crate::pipeline::avro::get_schema_fields_avro(path),
        FileType::Csv => get_schema_fields_csv(path, csv_has_header.unwrap_or(true)),
        FileType::Orc => get_schema_fields_orc(path),
        _ => bail!("schema is only supported for Parquet, Avro, CSV, and ORC files"),
    }
}

fn get_schema_fields_csv(path: &str, has_header: bool) -> Result<Vec<SchemaField>> {
    let ctx = SessionContext::new();
    let df = tokio::task::block_in_place(|| {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(ctx.read_csv(path, CsvReadOptions::new().has_header(has_header)))
    })
    .map_err(|e| eyre::eyre!("{e}"))?;
    let schema = df.schema();
    Ok(schema_fields_from_arrow(schema.as_ref()))
}

fn get_schema_fields_orc(path: &str) -> Result<Vec<SchemaField>> {
    let file = File::open(path)?;
    let arrow_reader = ArrowReaderBuilder::try_new(file)?.build();
    let schema = arrow_reader.schema();
    Ok(schema_fields_from_arrow(schema.as_ref()))
}

fn print_schema_csv(fields: &[SchemaField]) -> Result<()> {
    for f in fields {
        let nullable = if f.nullable { ", nullable" } else { "" };
        let ct = f
            .converted_type
            .as_ref()
            .map(|c| format!(" ({c})"))
            .unwrap_or_default();
        println!(
            "{name}: {data_type}{ct}{nullable}",
            name = f.name,
            data_type = f.data_type,
        );
    }
    Ok(())
}

fn print_schema_json(fields: &[SchemaField], sparse: bool) -> Result<()> {
    let json = if sparse {
        serde_json::to_string(fields)?
    } else {
        let full: Vec<SchemaFieldFull> = fields.iter().map(SchemaFieldFull::from).collect();
        serde_json::to_string(&full)?
    };
    println!("{json}");
    Ok(())
}

fn print_schema_json_pretty(fields: &[SchemaField], sparse: bool) -> Result<()> {
    let json = if sparse {
        serde_json::to_string_pretty(fields)?
    } else {
        let full: Vec<SchemaFieldFull> = fields.iter().map(SchemaFieldFull::from).collect();
        serde_json::to_string_pretty(&full)?
    };
    println!("{json}");
    Ok(())
}

fn print_schema_yaml(fields: &[SchemaField], sparse: bool) -> Result<()> {
    let yaml_fields: Vec<Yaml<'static>> =
        fields.iter().map(|f| f.to_yaml_mapping(sparse)).collect();
    let doc = Yaml::Sequence(yaml_fields);
    let mut out = String::new();
    let mut emitter = YamlEmitter::new(&mut out);
    emitter.dump(&doc)?;
    println!("{out}");
    Ok(())
}

/// Prints schema fields in the specified output format.
pub fn print_schema_fields(
    fields: &[SchemaField],
    output: DisplayOutputFormat,
    sparse: bool,
) -> Result<()> {
    match output {
        DisplayOutputFormat::Csv => print_schema_csv(fields),
        DisplayOutputFormat::Json => print_schema_json(fields, sparse),
        DisplayOutputFormat::JsonPretty => print_schema_json_pretty(fields, sparse),
        DisplayOutputFormat::Yaml => print_schema_yaml(fields, sparse),
    }
}
