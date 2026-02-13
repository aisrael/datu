//! Contains shared types for the `datu` CLI and implementation.

use std::str::FromStr;

use clap::Args;

/// Output format for the schema command
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

/// schema command arguments
#[derive(Args)]
pub struct SchemaArgs {
    /// Path to the Parquet or Avro file
    pub file: String,
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
}

/// head and tail command arguments
#[derive(Args)]
pub struct HeadsOrTails {
    pub input: String,
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
}
