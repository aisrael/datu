//! Contains shared types for the `dtfu` CLI and implementation.

use clap::Args;
use clap::Subcommand;

/// The `dtfu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    Convert(ConvertArgs),
    Head(HeadsOrTails),
    Tail(HeadsOrTails),
    Version,
}

/// head and tail command arguments (shared)
#[derive(Args)]
pub struct HeadsOrTails {
    pub input: String,
    #[arg(short = 'n', long, default_value_t = 10, help = "Number of lines to print.")]
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
