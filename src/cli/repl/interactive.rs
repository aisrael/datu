//! Line editor, history, and main REPL loop.

use std::path::PathBuf;

use flt::ast::Expr;
use flt::parser::parse_expr;
use flt::repl::ReplHandler;

use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::plan::collect_pipe_stages;
use super::plan::is_statement_complete;
use super::plan::plan_pipeline_with_state;
use super::plan::validate_repl_pipeline_stages;
use super::stage::ReplPipelineStage;
use crate::cli::DisplayOutputFormat;

/// datu's [`ReplHandler`]: accumulates flt expressions into pipelines and executes them.
#[derive(Default)]
pub struct DatuRepl {
    pub statement_incomplete: bool,
    /// Accumulated expressions until a terminal stage (test assertions).
    pub(crate) pending_exprs: Vec<Expr>,
}

impl DatuRepl {
    /// Prints a list of available REPL commands.
    fn handle_help() {
        println!("Available commands:");
        println!("  /help (/h)   Show this help message");
        println!("  /quit (/q)   Exit the REPL");
    }

    /// Accumulates REPL input expressions until a terminal stage is reached.
    /// Returns a planned pipeline only when the accumulated statement is complete.
    pub(crate) fn eval_incremental(
        &mut self,
        expr: Expr,
    ) -> crate::Result<Option<Vec<ReplPipelineStage>>> {
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        self.pending_exprs.extend(exprs);

        let statement_complete = is_statement_complete(&self.pending_exprs);
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
    pub(crate) fn execute_pipeline(&mut self, stages: Vec<ReplPipelineStage>) -> crate::Result<()> {
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

impl ReplHandler for DatuRepl {
    fn eval(&mut self, line: &str) -> eyre::Result<()> {
        match parse_expr(line) {
            Ok((remainder, expr)) => {
                let remainder = remainder.trim();
                if remainder.is_empty() {
                    match self.eval_incremental(expr) {
                        Ok(Some(stages)) => {
                            let stage_strings: Vec<String> =
                                stages.iter().map(|s| s.to_string()).collect();
                            println!("Pipeline: {}", stage_strings.join(" |> "));
                            if let Err(e) = self.execute_pipeline(stages) {
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
