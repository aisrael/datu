//! datu - a data multi-tool CLI

use clap::Parser;
use clap::Subcommand;

mod commands;

use commands::convert;
use commands::head;
use commands::schema;
use commands::tail;

use crate::commands::convert::ConvertArgs;

/// datu - a data multi-tool
#[derive(Parser)]
#[command(name = "datu")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The `datu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    /// convert between file formats
    Convert(ConvertArgs),
    /// print the first n lines of a file
    Head(datu::cli::HeadsOrTails),
    /// print the last n lines of a file
    Tail(datu::cli::HeadsOrTails),
    /// display the schema of a file
    Schema(datu::cli::SchemaArgs),
    /// print the datu version
    Version,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Convert(args) => convert(args),
        Command::Head(args) => head(args),
        Command::Schema(args) => schema(args),
        Command::Tail(args) => tail(args),
        Command::Version => {
            println!("datu v{}", datu::VERSION);
            Ok(())
        }
    }
}
