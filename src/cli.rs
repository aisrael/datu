//! Contains shared types for the `dtfu` CLI and implementation.

use clap::Args;
use clap::Subcommand;

/// The `dtfu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    Convert(ConvertArgs),
    Version,
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
}
