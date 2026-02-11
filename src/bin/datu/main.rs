//! datu - a data multi-tool CLI

use clap::Parser;
use datu::cli::Command;

mod commands;

use commands::convert;
use commands::head;
use commands::schema;
use commands::tail;

/// datu - a data multi-tool
#[derive(Parser)]
#[command(name = "datu")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
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
