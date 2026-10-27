//! Line editor, history, and main REPL loop.

use std::path::PathBuf;

use flt::ast::Expr;
use flt::ast::Statement;
use flt::parser::parse_expr;
use flt::repl::ReplHandler;
use flt::runtime::Runtime;
use flt::runtime::SimpleRuntime;
use flt::runtime::Value;

use super::native::DatuPlan;
use super::native::display_plan;
use super::native::register_natives;
use super::plan::collect_pipe_stages;
use super::plan::fold_pipe_stages;
use super::plan::is_statement_complete;
use crate::cli::DisplayOutputFormat;
use crate::pipeline::PipelineBuilder;

/// Display options every REPL pipeline uses.
pub(super) fn apply_repl_display_options(builder: &mut PipelineBuilder) -> &mut PipelineBuilder {
    builder
        .sparse(true)
        // Pretty JSON forces the Arrow/record-batch writer so output is one parseable JSON
        // value (matches legacy REPL `write_batches`); DataFusion's compact JSON is NDJSON.
        .json_pretty(true)
        .display_format(DisplayOutputFormat::Csv)
        .display_csv_headers(true)
}

/// datu's [`ReplHandler`]: accumulates flt expressions into statements and evaluates them with
/// flt, which calls back into datu's natives.
pub struct DatuRepl {
    pub statement_incomplete: bool,
    /// Accumulated expressions until a terminal stage (test assertions).
    pub(crate) pending_exprs: Vec<Expr>,
    runtime: SimpleRuntime,
}

impl Default for DatuRepl {
    fn default() -> Self {
        let mut runtime = SimpleRuntime::default();
        // datu's native names are fixed and don't clash with flt's built-ins, so a failure here
        // is a programming error.
        register_natives(&mut runtime).expect("datu's natives should register");
        Self {
            statement_incomplete: false,
            pending_exprs: Vec::new(),
            runtime,
        }
    }
}

impl DatuRepl {
    /// Prints a list of available REPL commands.
    fn handle_help() {
        println!("Available commands:");
        println!("  /help (/h)   Show this help message");
        println!("  /quit (/q)   Exit the REPL");
    }

    /// Accumulates REPL input expressions until a terminal stage is reached.
    /// Returns the whole statement as one pipe expression once it's complete.
    pub(crate) fn eval_incremental(&mut self, expr: Expr) -> Option<Expr> {
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        self.pending_exprs.extend(exprs);

        let statement_complete = is_statement_complete(&self.pending_exprs);
        self.statement_incomplete = !statement_complete;
        if !statement_complete {
            return None;
        }
        fold_pipe_stages(std::mem::take(&mut self.pending_exprs))
    }

    /// Evaluates a statement with the flt runtime and shows its value: a plan's rows, nothing
    /// for `None` (`schema` and `write` have already printed or written), or the value itself.
    pub(crate) fn execute(&mut self, expr: Expr) -> crate::Result<()> {
        let value = self.runtime.eval_statement(&Statement::Expr(expr))?;
        match value.get_native::<DatuPlan>() {
            Some(plan) => display_plan(plan)?,
            None if matches!(value, Value::None) => {}
            None => println!("{value}"),
        }
        Ok(())
    }
}

impl ReplHandler for DatuRepl {
    fn eval(&mut self, line: &str) -> eyre::Result<()> {
        match parse_expr(line) {
            Ok((remainder, expr)) => {
                let remainder = remainder.trim();
                if remainder.is_empty() {
                    if let Some(statement) = self.eval_incremental(expr) {
                        println!("Pipeline: {statement}");
                        if let Err(e) = self.execute(statement) {
                            eprintln!("error: {e}");
                        }
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
        Ok(())
    }

    fn handle_command(&mut self, rest: &str) -> eyre::Result<bool> {
        let cmd = rest.split_whitespace().next().unwrap_or("");
        match cmd {
            "quit" | "q" => return Ok(false),
            "help" | "h" => Self::handle_help(),
            _ => eprintln!("unknown command: /{cmd}"),
        }
        Ok(true)
    }

    fn prompt(&self) -> &str {
        if self.statement_incomplete {
            "|> "
        } else {
            "> "
        }
    }
}

/// Where the datu REPL loads and saves its history.
pub fn repl_history_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|dir| dir.join("datu").join("history"))
}
