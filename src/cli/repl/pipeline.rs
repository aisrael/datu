//! Accumulated REPL expression state and execution via [`crate::pipeline::PipelineBuilder`].

use flt::ast::BinaryOp;
use flt::ast::Expr;

use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::plan::collect_pipe_stages;
use super::plan::is_terminal_expr;
use super::plan::plan_pipeline_with_state;
use super::plan::validate_repl_pipeline_stages;
use super::stage::PipelineStage;
use crate::Error;
use crate::cli::DisplayOutputFormat;

/// A general REPL pipeline planner.
pub struct ReplPipelinePlanner {
    pub statement_incomplete: bool,
    /// Accumulated expressions until a terminal stage (test assertions).
    pub(crate) pending_exprs: Vec<Expr>,
}

impl ReplPipelinePlanner {
    /// Creates an empty pipeline (output path tracked after `write` only).
    pub fn new() -> Self {
        Self {
            statement_incomplete: false,
            pending_exprs: Vec::new(),
        }
    }

    /// Constructs a pipeline from a datu REPL expression. Does not execute it.
    pub fn eval(&mut self, expr: Expr) -> crate::Result<Vec<PipelineStage>> {
        match expr {
            Expr::BinaryExpr(left, op, right) => self.eval_binary_expr(left, op, right),
            Expr::FunctionCall(name, args) => self.eval_exprs(vec![Expr::FunctionCall(name, args)]),
            _ => Err(Error::UnsupportedExpression(expr.to_string())),
        }
    }

    /// Evaluates a binary expression to a pipeline.
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
    pub fn eval_incremental(&mut self, expr: Expr) -> crate::Result<Option<Vec<PipelineStage>>> {
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
    pub async fn execute_pipeline(&mut self, stages: Vec<PipelineStage>) -> crate::Result<()> {
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
