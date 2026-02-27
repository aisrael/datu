//! REPL for datu

use anyhow::Result;
use datu::cli::repl::ReplContext;
use datu::cli::repl::eval;
use flt::parser::parse_expr;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

/// Maximum number of inputs to keep in REPL history.
const HISTORY_DEPTH: usize = 1000;

/// Runs the datu REPL.
pub async fn run() -> Result<()> {
    let config = rustyline::Config::builder()
        .max_history_size(HISTORY_DEPTH)?
        .auto_add_history(true)
        .build();
    let mut rl = DefaultEditor::with_config(config)?;
    let mut context = ReplContext {
        batches: None,
        writer: None,
    };
    repl(&mut rl, &mut context).await
}

/// Reads, evaluates, and prints in a loop until EOF.
async fn repl(rl: &mut DefaultEditor, context: &mut ReplContext) -> Result<()> {
    loop {
        // On success, we get the input line. On EOF, we break the loop.
        // On interrupt, we continue. On any other error, we return.
        let line = match rl.readline("> ") {
            Ok(line) => line,
            Err(ReadlineError::Eof) => break Ok(()),
            Err(ReadlineError::Interrupted) => continue,
            Err(e) => return Err(e.into()),
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match parse_expr(line) {
            Ok((remainder, expr)) => {
                let remainder = remainder.trim();
                if remainder.is_empty() {
                    eval(context, expr).await?;
                } else {
                    eprintln!(
                        "parse error: unexpected input after expression: {:?}",
                        remainder
                    );
                }
            }
            Err(e) => {
                eprintln!("parse error: {:?}", e);
            }
        }
    }
}
