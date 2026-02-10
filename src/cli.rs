//! Contains shared types for the `dtfu` CLI and implementation.

use std::str::FromStr;

use clap::Args;
use clap::Subcommand;

/// The `dtfu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    Convert(ConvertArgs),
    Head(HeadsOrTails),
    Tail(HeadsOrTails),
    Schema(SchemaArgs),
    Version,
}

/// Output format for the schema command
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisplayOutputType {
    #[default]
    Csv,
    Json,
    Yaml,
}

impl TryFrom<&str> for DisplayOutputType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(DisplayOutputType::Csv),
            "json" => Ok(DisplayOutputType::Json),
            "yaml" => Ok(DisplayOutputType::Yaml),
            _ => Err(format!(
                "unknown output type '{s}', expected csv, json, or yaml"
            )),
        }
    }
}

impl std::fmt::Display for DisplayOutputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisplayOutputType::Csv => write!(f, "csv"),
            DisplayOutputType::Json => write!(f, "json"),
            DisplayOutputType::Yaml => write!(f, "yaml"),
        }
    }
}

impl FromStr for DisplayOutputType {
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
        default_value_t = DisplayOutputType::Csv,
        value_parser = clap::value_parser!(DisplayOutputType),
        help = "Output format: csv, json, or yaml"
    )]
    pub output: DisplayOutputType,
}

/// head and tail command arguments (shared)
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
        help = "Columns to select. If not specified, all columns will be printed."
    )]
    pub select: Option<Vec<String>>,
}

/// convert command arguments
#[derive(Args)]
pub struct ConvertArgs {
    pub input: String,
    pub output: String,
    #[arg(
        long,
        help = "Columns to select. If not specified, all columns will be selected."
    )]
    pub select: Option<Vec<String>>,
    #[arg(long, help = "Maximum number of records to read from the input.")]
    pub limit: Option<usize>,
}
