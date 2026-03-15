//! REPL for datu

use std::path::PathBuf;

use anyhow::Result;
use datu::cli::repl::ReplPipelineBuilder;
use flt::parser::parse_expr;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

/// Maximum number of inputs to keep in REPL history.
const HISTORY_DEPTH: usize = 1000;

fn repl_history_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|dir| dir.join("datu").join("history"))
}

fn load_repl_history(rl: &mut DefaultEditor) -> Result<()> {
    let Some(history_path) = repl_history_path() else {
        return Ok(());
    };
    if history_path.exists() {
        println!("Loading REPL history from: {:?}", history_path);
        rl.load_history(&history_path)?;
    }
    Ok(())
}

fn save_repl_history(rl: &mut DefaultEditor) -> Result<()> {
    let Some(history_path) = repl_history_path() else {
        return Ok(());
    };
    if let Some(parent) = history_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    rl.save_history(&history_path)?;
    Ok(())
}

/// Runs the datu REPL.
pub async fn run() -> Result<()> {
    let config = rustyline::Config::builder()
        .max_history_size(HISTORY_DEPTH)?
        .auto_add_history(true)
        .build();
    let mut rl = DefaultEditor::with_config(config)?;
    let _ = load_repl_history(&mut rl);
    let mut context = ReplPipelineBuilder::new();
    let repl_result = repl(&mut rl, &mut context).await;
    let _ = save_repl_history(&mut rl);
    repl_result
}

/// Reads, evaluates, and prints in a loop until EOF.
async fn repl(rl: &mut DefaultEditor, context: &mut ReplPipelineBuilder) -> Result<()> {
    loop {
        // On success, we get the input line. On EOF, we break the loop.
        // On interrupt, we continue. On any other error, we return.
        let prompt = if context.statement_incomplete {
            "|> "
        } else {
            "> "
        };
        let line = match rl.readline(prompt) {
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
                    if let Some(pipeline) = context.eval_incremental(expr)? {
                        let stages: Vec<String> = pipeline.iter().map(|s| s.to_string()).collect();
                        println!("Pipeline: {}", stages.join(" |> "));
                        context.execute_pipeline(pipeline).await?;
                    }
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
