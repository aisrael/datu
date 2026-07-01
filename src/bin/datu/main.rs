//! datu - a data multi-tool CLI

use clap::Parser;
use clap::Subcommand;

mod commands;

use commands::HeadsOrTailsCmd;
use commands::concat;
use commands::convert;
use commands::count;
use commands::diff;
use commands::heads_or_tails;
use commands::schema;
use commands::split;
use datu::cli::repl::Repl;

use crate::commands::concat::ConcatArgs;
use crate::commands::convert::ConvertArgs;
use crate::commands::diff::DiffArgs;
use crate::commands::split::SplitArgs;

/// Top-level CLI structure that parses command-line arguments.
#[derive(Parser)]
#[command(name = "datu")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

/// The `datu` CLI top-level command
#[derive(Subcommand)]
pub enum Command {
    /// concatenate multiple input files into a single output file
    Concat(ConcatArgs),
    /// convert between file formats
    Convert(ConvertArgs),
    /// return the number of rows in a file
    Count(datu::cli::CountArgs),
    /// compare two data files row-by-row
    Diff(DiffArgs),
    /// print the first n lines of a file
    Head(datu::cli::HeadsOrTails),
    /// sample n random rows from a file
    Sample(datu::cli::HeadsOrTails),
    /// print the last n lines of a file
    Tail(datu::cli::HeadsOrTails),
    /// display the schema of a file
    Schema(datu::cli::SchemaArgs),
    /// split a large input file into multiple output files of at most N rows each
    Split(SplitArgs),
    /// print the datu version
    Version,
}

/// Application entry point; parses CLI args and dispatches to the appropriate command.
#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        None => run_repl().await,
        Some(Command::Concat(args)) => concat(args).await,
        Some(Command::Convert(args)) => convert(args).await,
        Some(Command::Count(args)) => count(args).await,
        Some(Command::Diff(args)) => diff(args).await,
        Some(Command::Head(args)) => heads_or_tails(args, HeadsOrTailsCmd::Head).await,
        Some(Command::Sample(args)) => heads_or_tails(args, HeadsOrTailsCmd::Sample).await,
        Some(Command::Schema(args)) => schema(args).await,
        Some(Command::Split(args)) => split(args).await,
        Some(Command::Tail(args)) => heads_or_tails(args, HeadsOrTailsCmd::Tail).await,
        Some(Command::Version) => {
            println!("datu v{}", datu::VERSION);
            Ok(())
        }
    }
}

/// Runs the datu REPL.
pub async fn run_repl() -> eyre::Result<()> {
    let mut repl = Repl::new()?;
    repl.run().await
}
