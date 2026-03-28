//! Line editor, history, and main REPL loop.

use std::path::PathBuf;

use flt::parser::parse_expr;
use rustyline::Config;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use super::pipeline::ReplPipelinePlanner;

/// Maximum number of inputs to keep in REPL history.
const REPL_HISTORY_DEPTH: usize = 1000;

/// Interactive REPL with its own line editor, history, and pipeline state.
pub struct Repl {
    editor: DefaultEditor,
    pipeline: ReplPipelinePlanner,
}

impl Repl {
    /// Creates a REPL with line editor config and loads history when available.
    pub fn new() -> eyre::Result<Self> {
        let config = Config::builder()
            .max_history_size(REPL_HISTORY_DEPTH)?
            .auto_add_history(true)
            .build();
        let mut editor = DefaultEditor::with_config(config)?;
        let _ = load_repl_history(&mut editor);
        Ok(Self {
            editor,
            pipeline: ReplPipelinePlanner::new(),
        })
    }

    /// Runs the interactive prompt until EOF or error; persists history on exit.
    pub async fn run(&mut self) -> eyre::Result<()> {
        let loop_result = self.repl_loop().await;
        let _ = save_repl_history(&mut self.editor);
        loop_result
    }

    /// The main REPL loop.
    async fn repl_loop(&mut self) -> eyre::Result<()> {
        loop {
            let prompt = if self.pipeline.statement_incomplete {
                "|> "
            } else {
                "> "
            };
            let line = match self.editor.readline(prompt) {
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
                        match self.pipeline.eval_incremental(expr) {
                            Ok(Some(pipeline)) => {
                                let stages: Vec<String> =
                                    pipeline.iter().map(|s| s.to_string()).collect();
                                println!("Pipeline: {}", stages.join(" |> "));
                                if let Err(e) = self.pipeline.execute_pipeline(pipeline).await {
                                    eprintln!("error: {e}");
                                }
                            }
                            Ok(None) => {}
                            Err(e) => eprintln!("error: {e}"),
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
}

fn repl_history_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|dir| dir.join("datu").join("history"))
}

fn load_repl_history(editor: &mut DefaultEditor) -> eyre::Result<()> {
    let Some(history_path) = repl_history_path() else {
        return Ok(());
    };
    if history_path.exists() {
        println!("Loading REPL history from: {:?}", history_path);
        editor.load_history(&history_path)?;
    }
    Ok(())
}

fn save_repl_history(editor: &mut DefaultEditor) -> eyre::Result<()> {
    let Some(history_path) = repl_history_path() else {
        return Ok(());
    };
    if let Some(parent) = history_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    editor.save_history(&history_path)?;
    Ok(())
}
