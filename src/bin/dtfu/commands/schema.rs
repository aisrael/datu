//! `dtfu schema` - display the schema of a Parquet or Avro file

use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::Result;
use anyhow::bail;
use arrow_avro::reader::ReaderBuilder;
use dtfu::FileType;
use dtfu::cli::DisplayOutputFormat;
use dtfu::cli::SchemaArgs;
use parquet::basic::ConvertedType;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::ColumnDescriptor;
use saphyr::Scalar;
use saphyr::Yaml;
use saphyr::YamlEmitter;
use serde::Serialize;

#[derive(Clone, Serialize)]
struct SchemaField {
    name: String,
    data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    converted_type: Option<String>,
    nullable: bool,
}

impl SchemaField {
    fn to_yaml_mapping(&self) -> Yaml<'static> {
        let mut map = hashlink::LinkedHashMap::new();
        map.insert(
            Yaml::scalar_from_string("name".to_string()),
            Yaml::scalar_from_string(self.name.clone()),
        );
        map.insert(
            Yaml::scalar_from_string("data_type".to_string()),
            Yaml::scalar_from_string(self.data_type.clone()),
        );
        if let Some(ref ct) = self.converted_type {
            map.insert(
                Yaml::scalar_from_string("converted_type".to_string()),
                Yaml::scalar_from_string(ct.clone()),
            );
        }
        map.insert(
            Yaml::scalar_from_string("nullable".to_string()),
            Yaml::Value(Scalar::Boolean(self.nullable)),
        );
        Yaml::Mapping(map)
    }
}

struct SchemaOutput {
    column_name: String,
    data_type: String,
    converted_type: Option<ConvertedType>,
    nullable: bool,
}

impl SchemaOutput {
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

fn print_schema(fields: &[SchemaField], output: DisplayOutputFormat) -> Result<()> {
    match output {
        DisplayOutputFormat::Csv => {
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
                    ct = ct,
                    nullable = nullable
                );
            }
        }
        DisplayOutputFormat::Json => {
            let json = serde_json::to_string(fields).map_err(anyhow::Error::from)?;
            println!("{json}");
        }
        DisplayOutputFormat::JsonPretty => {
            let json = serde_json::to_string_pretty(fields).map_err(anyhow::Error::from)?;
            println!("{json}");
        }
        DisplayOutputFormat::Yaml => {
            let yaml_fields: Vec<Yaml<'static>> =
                fields.iter().map(|f| f.to_yaml_mapping()).collect();
            let doc = Yaml::Sequence(yaml_fields);
            let mut out = String::new();
            let mut emitter = YamlEmitter::new(&mut out);
            emitter.dump(&doc).map_err(anyhow::Error::from)?;
            println!("{out}");
        }
    }
    Ok(())
}

fn schema_avro(path: &str, output: DisplayOutputFormat) -> Result<()> {
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
    print_schema(&fields, output)
}

/// The `dtfu schema` command
pub fn schema(args: SchemaArgs) -> Result<()> {
    let file_type: FileType = args.file.as_str().try_into()?;
    match file_type {
        FileType::Parquet => schema_parquet(&args.file, args.output),
        FileType::Avro => schema_avro(&args.file, args.output),
        _ => bail!("schema is only supported for Parquet and Avro files"),
    }
}

fn schema_parquet(path: &str, output: DisplayOutputFormat) -> Result<()> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(anyhow::Error::from)?;

    let file_metadata = metadata.file_metadata();
    let schema_descr = file_metadata.schema_descr();

    let columns: Vec<SchemaOutput> = schema_descr
        .columns()
        .iter()
        .map(column_to_schema_output)
        .collect();

    let fields: Vec<SchemaField> = columns.iter().map(SchemaOutput::to_schema_field).collect();
    print_schema(&fields, output)
}
