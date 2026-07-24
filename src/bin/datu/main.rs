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

/// Top-level subcommand names that are read-only queries; everything else listed
/// under the top-level `Commands:` help is treated as a mutating/creating command.
const QUERY_COMMANDS: &[&str] = &["count", "diff", "head", "sample", "tail", "schema"];

/// Builds the top-level `--help` text, splitting clap's default flat `Commands:`
/// listing into "Queries:" and "Commands:" sections based on [`QUERY_COMMANDS`].
fn grouped_help(cmd: &clap::Command) -> String {
    let mut plain = cmd.clone();
    plain = plain.color(clap::ColorChoice::Never);
    let rendered = plain.render_help().to_string();

    let lines: Vec<&str> = rendered.lines().collect();
    let Some(heading_idx) = lines.iter().position(|line| *line == "Commands:") else {
        return rendered;
    };
    let block_end = lines[heading_idx + 1..]
        .iter()
        .position(|line| line.trim().is_empty())
        .map(|offset| heading_idx + 1 + offset)
        .unwrap_or(lines.len());
    let block = &lines[heading_idx + 1..block_end];

    let mut queries = Vec::new();
    let mut commands = Vec::new();
    for line in block {
        let name = line.split_whitespace().next().unwrap_or("");
        if QUERY_COMMANDS.contains(&name) {
            queries.push(*line);
        } else {
            commands.push(*line);
        }
    }

    let mut out: Vec<&str> = Vec::new();
    out.extend_from_slice(&lines[..heading_idx]);
    out.push("Queries:");
    out.extend_from_slice(&queries);
    out.push("");
    out.push("Commands:");
    out.extend_from_slice(&commands);
    out.extend_from_slice(&lines[block_end..]);

    out.join("\n")
}

/// Application entry point; parses CLI args and dispatches to the appropriate command.
#[tokio::main]
async fn main() -> eyre::Result<()> {
    use clap::{CommandFactory, FromArgMatches};

    let mut cmd = Cli::command();
    let help_text = grouped_help(&cmd);
    cmd = cmd.override_help(help_text);
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches)?;

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
