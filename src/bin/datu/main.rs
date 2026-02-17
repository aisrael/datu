//! datu - a data multi-tool CLI

use clap::Parser;
use clap::Subcommand;

mod commands;

use commands::convert;
use commands::count;
use commands::head;
use commands::schema;
use commands::tail;

use crate::commands::convert::ConvertArgs;

/// Top-level CLI structure that parses command-line arguments.
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
    /// return the number of rows in a file
    Count(datu::cli::CountArgs),
    /// print the first n lines of a file
    Head(datu::cli::HeadsOrTails),
    /// print the last n lines of a file
    Tail(datu::cli::HeadsOrTails),
    /// display the schema of a file
    Schema(datu::cli::SchemaArgs),
    /// print the datu version
    Version,
}

/// Application entry point; parses CLI args and dispatches to the appropriate command.
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Convert(args) => convert(args),
        Command::Count(args) => count(args),
        Command::Head(args) => head(args),
        Command::Schema(args) => schema(args),
        Command::Tail(args) => tail(args),
        Command::Version => {
            println!("datu v{}", datu::VERSION);
            Ok(())
        }
    }
}
