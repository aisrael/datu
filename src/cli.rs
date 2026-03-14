//! Contains shared types for the `datu` CLI and implementation.

pub mod repl;

use std::str::FromStr;

use clap::Args;

use crate::FileType;

/// Output format for schema, head, and tail commands (csv, json, json-pretty, yaml).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisplayOutputFormat {
    #[default]
    Csv,
    Json,
    JsonPretty,
    Yaml,
}

impl TryFrom<&str> for DisplayOutputFormat {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(DisplayOutputFormat::Csv),
            "json" => Ok(DisplayOutputFormat::Json),
            "json-pretty" => Ok(DisplayOutputFormat::JsonPretty),
            "yaml" => Ok(DisplayOutputFormat::Yaml),
            _ => Err(format!(
                "unknown output type '{s}', expected csv, json, json-pretty, or yaml"
            )),
        }
    }
}

impl std::fmt::Display for DisplayOutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisplayOutputFormat::Csv => write!(f, "csv"),
            DisplayOutputFormat::Json => write!(f, "json"),
            DisplayOutputFormat::JsonPretty => write!(f, "json-pretty"),
            DisplayOutputFormat::Yaml => write!(f, "yaml"),
        }
    }
}

impl FromStr for DisplayOutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

/// Arguments for the `datu schema` command.
#[derive(Args)]
pub struct SchemaArgs {
    /// Path to the Parquet, Avro, ORC, or CSV file
    pub input_path: String,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub input: Option<FileType>,
    #[arg(
        long,
        short,
        default_value_t = DisplayOutputFormat::Csv,
        value_parser = clap::value_parser!(DisplayOutputFormat),
        help = "Output format: csv, json, json-pretty, or yaml"
    )]
    pub output: DisplayOutputFormat,
    #[arg(
        long,
        default_value_t = true,
        help = "For JSON/YAML: omit keys with null/missing values. Default: true. Use --sparse=false to include default values."
    )]
    pub sparse: bool,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV input: whether the first row is a header. Default: true when omitted. Use --input-headers=false for headerless CSV."
    )]
    pub input_headers: Option<bool>,
}

/// Arguments for the `datu count` command.
#[derive(Args)]
pub struct CountArgs {
    /// Path to the Parquet, Avro, ORC, or CSV file
    pub input_path: String,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub input: Option<FileType>,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV input: whether the first row is a header. Default: true when omitted. Use --input-headers=false for headerless CSV."
    )]
    pub input_headers: Option<bool>,
}

/// Arguments for the `datu head` and `datu tail` commands.
#[derive(Args)]
pub struct HeadsOrTails {
    /// Path to the Parquet, Avro, ORC, or CSV file
    pub input_path: String,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub input: Option<FileType>,
    #[arg(
        short = 'n',
        long,
        default_value_t = 10,
        help = "Number of lines to print."
    )]
    pub number: usize,
    #[arg(
        long,
        short,
        default_value_t = DisplayOutputFormat::Csv,
        value_parser = clap::value_parser!(DisplayOutputFormat),
        help = "Output format: csv, json, json-pretty, or yaml"
    )]
    pub output: DisplayOutputFormat,
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "For JSON/YAML: omit keys with null/missing values. Default: true. Use --sparse=false to include default values."
    )]
    pub sparse: bool,
    #[arg(
        long,
        help = "Columns to select. If not specified, all columns will be printed."
    )]
    pub select: Option<Vec<String>>,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV input: whether the first row is a header. Default: true when omitted. Use --input-headers=false for headerless CSV."
    )]
    pub input_headers: Option<bool>,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV output: whether to print column headers. Default: true when omitted. Use --output-headers=false to suppress headers."
    )]
    pub output_headers: Option<bool>,
}
