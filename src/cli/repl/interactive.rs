//! Line editor, history, and main REPL loop.

use std::path::PathBuf;

#[cfg(test)]
use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::parser::parse_expr;
use rustyline::Config;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::plan::collect_pipe_stages;
use super::plan::is_terminal_expr;
use super::plan::plan_pipeline_with_state;
use super::plan::validate_repl_pipeline_stages;
use super::stage::PipelineStage;
#[cfg(test)]
use crate::Error;
use crate::cli::DisplayOutputFormat;

/// Maximum number of inputs to keep in REPL history.
const REPL_HISTORY_DEPTH: usize = 1000;

/// Interactive REPL with its own line editor, history, and pipeline state.
pub struct Repl {
    editor: DefaultEditor,
    pub statement_incomplete: bool,
    /// Accumulated expressions until a terminal stage (test assertions).
    pub(crate) pending_exprs: Vec<Expr>,
}

enum ReadlineResult {
    Empty,
    Line(String),
    Eof,
    Interrupt,
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
            statement_incomplete: false,
            pending_exprs: Vec::new(),
        })
    }

    /// Like [`Repl::new`] but skips loading REPL history (for unit tests).
    #[cfg(test)]
    pub(crate) fn new_for_tests() -> eyre::Result<Self> {
        let config = Config::builder()
            .max_history_size(REPL_HISTORY_DEPTH)?
            .auto_add_history(true)
            .build();
        let editor = DefaultEditor::with_config(config)?;
        Ok(Self {
            editor,
            statement_incomplete: false,
            pending_exprs: Vec::new(),
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
            match self.readline()? {
                ReadlineResult::Empty => continue,
                ReadlineResult::Line(line) => self.handle_line(&line).await,
                ReadlineResult::Eof => break Ok(()),
                ReadlineResult::Interrupt => continue,
            }
        }
    }

    /// Reads a line of input from the editor and returns a result.
    fn readline(&mut self) -> eyre::Result<ReadlineResult> {
        let prompt = if self.statement_incomplete {
            "|> "
        } else {
            "> "
        };
        match self.editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    Ok(ReadlineResult::Empty)
                } else {
                    Ok(ReadlineResult::Line(trimmed.to_string()))
                }
            }
            Err(ReadlineError::Eof) => Ok(ReadlineResult::Eof),
            Err(ReadlineError::Interrupted) => Ok(ReadlineResult::Interrupt),
            Err(e) => Err(e.into()),
        }
    }

    /// Handles a line of input in the REPL
    async fn handle_line(&mut self, line: &str) {
        match parse_expr(line) {
            Ok((remainder, expr)) => {
                let remainder = remainder.trim();
                if remainder.is_empty() {
                    match self.eval_incremental(expr) {
                        Ok(Some(stages)) => {
                            let stage_strings: Vec<String> =
                                stages.iter().map(|s| s.to_string()).collect();
                            println!("Pipeline: {}", stage_strings.join(" |> "));
                            if let Err(e) = self.execute_pipeline(stages).await {
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

    /// Constructs a pipeline from a datu REPL expression. Does not execute it.
    #[cfg(test)]
    pub(crate) fn eval(&mut self, expr: Expr) -> crate::Result<Vec<PipelineStage>> {
        match expr {
            Expr::BinaryExpr(left, op, right) => self.eval_binary_expr(left, op, right),
            Expr::FunctionCall(name, args) => self.eval_exprs(vec![Expr::FunctionCall(name, args)]),
            _ => Err(Error::UnsupportedExpression(expr.to_string())),
        }
    }

    /// Evaluates a binary expression to a pipeline.
    #[cfg(test)]
    #[allow(clippy::boxed_local)]
    fn eval_binary_expr(
        &mut self,
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    ) -> crate::Result<Vec<PipelineStage>> {
        match op {
            BinaryOp::Pipe => {
                // Flatten nested `|>` into a single ordered list (e.g. `a |> b |> c` is parsed as a
                // binary tree); each leaf becomes one pipeline stage for planning.
                let mut exprs = Vec::new();
                collect_pipe_stages(*left, &mut exprs);
                collect_pipe_stages(*right, &mut exprs);
                self.eval_exprs(exprs)
            }
            _ => Err(Error::UnsupportedOperator(op.to_string())),
        }
    }

    #[cfg(test)]
    fn eval_exprs(&mut self, exprs: Vec<Expr>) -> crate::Result<Vec<PipelineStage>> {
        let (stages, statement_incomplete) = plan_pipeline_with_state(exprs)?;
        self.statement_incomplete = statement_incomplete;
        if !statement_incomplete {
            validate_repl_pipeline_stages(&stages)?;
        }
        Ok(stages)
    }

    /// Accumulates REPL input expressions until a terminal stage is reached.
    /// Returns a planned pipeline only when the accumulated statement is complete.
    pub(crate) fn eval_incremental(
        &mut self,
        expr: Expr,
    ) -> crate::Result<Option<Vec<PipelineStage>>> {
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        self.pending_exprs.extend(exprs);

        let statement_complete = self.pending_exprs.last().is_some_and(is_terminal_expr);
        self.statement_incomplete = !statement_complete;
        if !statement_complete {
            return Ok(None);
        }

        let planned = plan_pipeline_with_state(std::mem::take(&mut self.pending_exprs))?;
        self.statement_incomplete = planned.1;
        validate_repl_pipeline_stages(&planned.0)?;
        Ok(Some(planned.0))
    }

    /// Executes a planned pipeline via [`PipelineBuilder`] (same path as CLI `head` / `convert`).
    pub(crate) async fn execute_pipeline(
        &mut self,
        stages: Vec<PipelineStage>,
    ) -> crate::Result<()> {
        validate_repl_pipeline_stages(&stages)?;
        let mut builder = repl_stages_to_pipeline_builder(&stages)?;
        builder
            .sparse(true)
            // Pretty JSON forces the Arrow/record-batch writer so output is one parseable JSON
            // value (matches legacy REPL `write_batches`); DataFusion's compact JSON is NDJSON.
            .json_pretty(true)
            .display_format(DisplayOutputFormat::Csv)
            .display_csv_headers(true);
        let mut pipeline = builder.build()?;
        pipeline.execute()?;
        Ok(())
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
