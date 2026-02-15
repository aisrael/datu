//! `datu schema` - display the schema of a Parquet, Avro, or ORC file

use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::Result;
use anyhow::bail;
use arrow::array::RecordBatchReader;
use arrow_avro::reader::ReaderBuilder;
use datu::FileType;
use datu::cli::DisplayOutputFormat;
use datu::cli::SchemaArgs;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::ColumnDescriptor;
use saphyr::Scalar;
use saphyr::Yaml;
use saphyr::YamlEmitter;
use serde::Serialize;

/// A schema field with name, data type, converted type (if any), and nullability.
#[derive(Clone, Serialize)]
struct SchemaField {
    name: String,
    data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    converted_type: Option<String>,
    nullable: bool,
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
    /// Creates a full schema field from a sparse schema field.
    fn from(f: &SchemaField) -> Self {
        SchemaFieldFull {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            converted_type: f.converted_type.clone(),
            nullable: f.nullable,
        }
    }
}

/// Internal representation of a schema column from Parquet metadata.
struct SchemaOutput {
    column_name: String,
    data_type: String,
    converted_type: Option<ConvertedType>,
    nullable: bool,
}

impl SchemaOutput {
    /// Converts this Parquet schema output into a `SchemaField` for display.
    fn to_schema_field(&self) -> SchemaField {
        SchemaField {
            name: self.column_name.clone(),
            data_type: self.data_type.clone(),
            converted_type: self.converted_type.as_ref().map(|ct| format!("{ct:?}")),
            nullable: self.nullable,
        }
    }
}

impl Display for SchemaOutput {
    /// Formats the schema output for human-readable display.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nullable = if self.nullable { ", nullable" } else { "" };
        if let Some(converted_type) = &self.converted_type {
            write!(
                f,
                "{}: {} ({:?}){}",
                self.column_name, self.data_type, converted_type, nullable
            )
        } else {
            write!(f, "{}: {}{}", self.column_name, self.data_type, nullable)
        }
    }
}

/// Map an element of `SchemaDescriptor::columns()` into a `SchemaOutput`
fn column_to_schema_output(column: &Arc<ColumnDescriptor>) -> SchemaOutput {
    let path = column.path();
    let physical_type = column.physical_type();
    let logical_type = column.logical_type_ref();
    let converted_type = column.converted_type();

    let column_name = path.parts().join(".");

    let data_type = if let Some(logical) = logical_type {
        format!("{:?}", logical)
    } else {
        format!("{}", physical_type)
    };

    let converted_type = if matches!(converted_type, ConvertedType::NONE) {
        None
    } else {
        Some(converted_type)
    };

    // A column is nullable if max_def_level > 0
    // max_def_level == 0 means the column is required (not nullable)
    let nullable = column.max_def_level() > 0;

    SchemaOutput {
        column_name,
        data_type,
        converted_type,
        nullable,
    }
}

/// Prints schema fields in CSV-like format (one field per line).
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

/// Prints schema fields as compact JSON.
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

/// Prints schema fields as pretty-printed JSON.
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

/// Prints schema fields as YAML.
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
fn print_schema(fields: &[SchemaField], output: DisplayOutputFormat, sparse: bool) -> Result<()> {
    match output {
        DisplayOutputFormat::Csv => print_schema_csv(fields),
        DisplayOutputFormat::Json => print_schema_json(fields, sparse),
        DisplayOutputFormat::JsonPretty => print_schema_json_pretty(fields, sparse),
        DisplayOutputFormat::Yaml => print_schema_yaml(fields, sparse),
    }
}

/// Extracts and prints the schema of an Avro file.
fn schema_avro(path: &str, output: DisplayOutputFormat, sparse: bool) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let arrow_reader = ReaderBuilder::new().build(reader)?;
    let schema = arrow_reader.schema();
    let fields: Vec<SchemaField> = schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            converted_type: None,
            nullable: f.is_nullable(),
        })
        .collect();
    print_schema(&fields, output, sparse)
}

/// The `datu schema` command
pub fn schema(args: SchemaArgs) -> Result<()> {
    let file_type: FileType = args.file.as_str().try_into()?;
    match file_type {
        FileType::Parquet => schema_parquet(&args.file, args.output, args.sparse),
        FileType::Avro => schema_avro(&args.file, args.output, args.sparse),
        FileType::Orc => schema_orc(&args.file, args.output, args.sparse),
        _ => bail!("schema is only supported for Parquet, Avro, and ORC files"),
    }
}

/// Extracts and prints the schema of an ORC file.
fn schema_orc(path: &str, output: DisplayOutputFormat, sparse: bool) -> Result<()> {
    let file = File::open(path)?;
    let arrow_reader = ArrowReaderBuilder::try_new(file)?.build();
    let schema = arrow_reader.schema();
    let fields: Vec<SchemaField> = schema
        .fields()
        .iter()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            converted_type: None,
            nullable: f.is_nullable(),
        })
        .collect();
    print_schema(&fields, output, sparse)
}

/// Extracts and prints the schema of a Parquet file.
fn schema_parquet(path: &str, output: DisplayOutputFormat, sparse: bool) -> Result<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;

    let file_metadata = metadata.file_metadata();
    let schema_descr = file_metadata.schema_descr();

    let columns: Vec<SchemaOutput> = schema_descr
        .columns()
        .iter()
        .map(column_to_schema_output)
        .collect();

    let fields: Vec<SchemaField> = columns.iter().map(SchemaOutput::to_schema_field).collect();
    print_schema(&fields, output, sparse)
}
