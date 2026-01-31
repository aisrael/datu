//! dtfu - a data multi-tool CLI

use clap::Parser;

/// dtfu - a data multi-tool
#[derive(Parser)]
#[command(name = "dtfu")]
#[command(version, about, long_about = None)]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
    println!("Hello, world!");
}
