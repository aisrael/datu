use std::fmt;
use std::path::PathBuf;

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;
use flt::parser::parse_expr;
use rustyline::Config;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::Error;
use crate::FileType;
use crate::cli::DisplayOutputFormat;
pub use crate::pipeline::ColumnSpec;
use crate::pipeline::Source;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::count_rows;
use crate::pipeline::display::write_record_batches_as_csv;
use crate::pipeline::read_to_batches;
use crate::pipeline::sample_batches;
use crate::pipeline::schema;
use crate::pipeline::select;
use crate::pipeline::tail_batches;
use crate::pipeline::write_batches;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
pub enum PipelineStage {
    Read {
        path: String,
    },
    Select {
        columns: Vec<ColumnSpec>,
    },
    Head {
        n: usize,
    },
    Tail {
        n: usize,
    },
    Sample {
        n: usize,
    },
    /// Count rows. `Some(path)` = count file directly (efficient); `None` = sum batches in context.
    Count {
        path: Option<String>,
    },
    Schema,
    Write {
        path: String,
    },
    Print,
}

impl PipelineStage {
    /// Returns true when a stage closes a REPL statement.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipelineStage::Count { .. }
                | PipelineStage::Head { .. }
                | PipelineStage::Tail { .. }
                | PipelineStage::Sample { .. }
                | PipelineStage::Schema
                | PipelineStage::Write { .. }
        )
    }

    /// Returns true for stages that can continue to another explicit stage.
    pub fn is_non_terminal(&self) -> bool {
        !self.is_terminal()
    }

    /// Returns any implicit stage that should be appended after this stage.
    pub fn implicit_followup_stage(&self) -> Option<PipelineStage> {
        match self {
            PipelineStage::Head { .. }
            | PipelineStage::Tail { .. }
            | PipelineStage::Sample { .. } => Some(PipelineStage::Print),
            _ => None,
        }
    }
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineStage::Read { path } => write!(f, r#"read("{path}")"#),
            PipelineStage::Select { columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| match c {
                        ColumnSpec::Exact(s) => format!(r#""{s}""#),
                        ColumnSpec::CaseInsensitive(s) => format!(":{s}"),
                    })
                    .collect::<Vec<_>>();
                write!(f, "select({})", cols.join(", "))
            }
            PipelineStage::Head { n } => write!(f, "head({n})"),
            PipelineStage::Tail { n } => write!(f, "tail({n})"),
            PipelineStage::Sample { n } => write!(f, "sample({n})"),
            PipelineStage::Count { path: Some(p) } => write!(f, r#"count("{p}")"#),
            PipelineStage::Count { path: None } => write!(f, "count()"),
            PipelineStage::Schema => write!(f, "schema()"),
            PipelineStage::Write { path } => write!(f, r#"write("{path}")"#),
            PipelineStage::Print => write!(f, "print()"),
        }
    }
}

/// A general REPL pipeline.
pub struct ReplPipeline {
    pub batches: Option<Vec<arrow::record_batch::RecordBatch>>,
    pub writer: Option<String>,
    pub statement_incomplete: bool,
    pending_exprs: Vec<Expr>,
}

impl Default for ReplPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplPipeline {
    pub fn new() -> Self {
        Self {
            batches: None,
            writer: None,
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
        Ok(Some(planned.0))
    }

    /// Executes a planned pipeline.
    pub async fn execute_pipeline(&mut self, stages: Vec<PipelineStage>) -> crate::Result<()> {
        for stage in stages {
            self.execute_stage(stage).await?;
        }
        Ok(())
    }

    /// Dispatches a single planned stage to the appropriate execution method.
    async fn execute_stage(&mut self, stage: PipelineStage) -> crate::Result<()> {
        match stage {
            PipelineStage::Read { path } => self.exec_read(&path).await,
            PipelineStage::Select { columns } => self.exec_select(&columns).await,
            PipelineStage::Head { n } => self.exec_head(n),
            PipelineStage::Tail { n } => self.exec_tail(n),
            PipelineStage::Sample { n } => self.exec_sample(n),
            PipelineStage::Count { path: Some(p) } => self.exec_count_path(&p),
            PipelineStage::Count { path: None } => self.exec_count(),
            PipelineStage::Schema => self.exec_schema(),
            PipelineStage::Write { path } => self.exec_write(&path).await,
            PipelineStage::Print => self.print_batches(),
        }
    }

    /// Reads a file into record batches.
    async fn exec_read(&mut self, path: &str) -> crate::Result<()> {
        let file_type: FileType = path.try_into()?;
        let batches = read_to_batches(path, file_type, &None, None, None)
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?;
        self.batches = Some(batches);
        Ok(())
    }

    /// Selects columns from the batches in context.
    async fn exec_select(&mut self, columns: &[ColumnSpec]) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("select requires a preceding read in the pipe".to_string())
        })?;
        let selected = select::select_columns_to_batches(batches, columns)?;
        self.batches = Some(selected);
        Ok(())
    }

    /// Takes the first N rows from the batches in context.
    fn exec_head(&mut self, n: usize) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("head requires a preceding read in the pipe".to_string())
        })?;
        let mut result = Vec::new();
        let mut remaining = n;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let rows = batch.num_rows().min(remaining);
            result.push(batch.slice(0, rows));
            remaining -= rows;
        }
        self.batches = Some(result);
        Ok(())
    }

    /// Takes the last N rows from the batches in context.
    fn exec_tail(&mut self, n: usize) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("tail requires a preceding read in the pipe".to_string())
        })?;
        let result = tail_batches(batches, n);
        self.batches = Some(result);
        Ok(())
    }

    /// Samples N random rows from the batches in context.
    fn exec_sample(&mut self, n: usize) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("sample requires a preceding read in the pipe".to_string())
        })?;
        let result = sample_batches(batches, n);
        self.batches = Some(result);
        Ok(())
    }

    /// Counts rows in a file directly (metadata for Parquet/ORC, streaming for Avro/CSV).
    fn exec_count_path(&mut self, path: &str) -> crate::Result<()> {
        let file_type: FileType = path.try_into()?;
        let total =
            count_rows(path, file_type, None).map_err(|e| Error::GenericError(e.to_string()))?;
        println!("{total}");
        Ok(())
    }

    /// Counts the total number of rows across all batches and prints the result.
    fn exec_count(&mut self) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("count requires a preceding read in the pipe".to_string())
        })?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("{total}");
        Ok(())
    }

    /// Prints the schema of the batches in context.
    fn exec_schema(&mut self) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("schema requires a preceding read in the pipe".to_string())
        })?;
        let first = batches
            .first()
            .ok_or_else(|| Error::GenericError("schema requires non-empty batches".to_string()))?;
        let fields = schema::schema_fields_from_arrow(first.schema().as_ref());
        schema::print_schema_fields(&fields, DisplayOutputFormat::Csv, true)
            .map_err(|e| Error::GenericError(e.to_string()))
    }

    /// Writes the batches in context to an output file.
    async fn exec_write(&mut self, output_path: &str) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("write requires a preceding read in the pipe".to_string())
        })?;
        let output_file_type: FileType = output_path.try_into()?;
        write_batches(batches, output_path, output_file_type, true, false)
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?;
        self.writer = Some(output_path.to_string());
        Ok(())
    }

    /// Prints batches from the context as CSV to stdout (implicit `print(:csv)`).
    fn print_batches(&mut self) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("print requires batches in the context".to_string())
        })?;
        let mut source = VecRecordBatchReaderSource::new(batches);
        let mut reader = source.get()?;
        write_record_batches_as_csv(&mut *reader, std::io::stdout(), true)
    }
}

// --- Interactive REPL -------------------------------------------------------

/// Maximum number of inputs to keep in REPL history.
const REPL_HISTORY_DEPTH: usize = 1000;

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

/// Interactive REPL with its own line editor, history, and pipeline state.
pub struct Repl {
    editor: DefaultEditor,
    pipeline: ReplPipeline,
}

impl Repl {
    pub fn new() -> eyre::Result<Self> {
        let config = Config::builder()
            .max_history_size(REPL_HISTORY_DEPTH)?
            .auto_add_history(true)
            .build();
        let mut editor = DefaultEditor::with_config(config)?;
        let _ = load_repl_history(&mut editor);
        Ok(Self {
            editor,
            pipeline: ReplPipeline::new(),
        })
    }

    pub async fn run(&mut self) -> eyre::Result<()> {
        let loop_result = self.repl_loop().await;
        let _ = save_repl_history(&mut self.editor);
        loop_result
    }

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
                        if let Some(pipeline) = self.pipeline.eval_incremental(expr)? {
                            let stages: Vec<String> =
                                pipeline.iter().map(|s| s.to_string()).collect();
                            println!("Pipeline: {}", stages.join(" |> "));
                            self.pipeline.execute_pipeline(pipeline).await?;
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

/// Runs the datu REPL.
pub async fn run() -> eyre::Result<()> {
    let mut repl = Repl::new()?;
    repl.run().await
}

/// Collects pipeline stages from a pipe expression (e.g. a |> b |> c -> [a, b, c]).
fn collect_pipe_stages(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(l, BinaryOp::Pipe, r) => {
            collect_pipe_stages(*l, out);
            collect_pipe_stages(*r, out);
        }
        other => out.push(other),
    }
}

fn is_terminal_expr(expr: &Expr) -> bool {
    if let Expr::FunctionCall(name, _) = expr {
        matches!(
            name.to_string().as_str(),
            "count" | "head" | "tail" | "sample" | "schema" | "write"
        )
    } else {
        false
    }
}

/// Extracts a single path string from a function's argument list.
fn extract_path_from_args(func_name: &str, args: &[Expr]) -> crate::Result<String> {
    match args {
        [Expr::Literal(Literal::String(s))] => Ok(s.clone()),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "{func_name} expects a single string argument, got {args:?}"
        ))),
    }
}

/// Extracts column specs from select args. Symbols (:one) and identifiers use case-insensitive
/// match; strings ("one") use exact match.
fn extract_column_specs(args: &[Expr]) -> crate::Result<Vec<ColumnSpec>> {
    args.iter()
        .map(|expr| match expr {
            Expr::Literal(Literal::Symbol(s)) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
            Expr::Literal(Literal::String(s)) => Ok(ColumnSpec::Exact(s.clone())),
            Expr::Ident(s) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "select expects symbol or string column names, got {expr:?}"
            ))),
        })
        .collect()
}

#[cfg(test)]
fn is_head_call(expr: Option<&Expr>) -> bool {
    if let Some(Expr::FunctionCall(name, _)) = expr {
        *name == "head"
    } else {
        false
    }
}

/// Plans a single pipeline stage from an AST expression.
fn plan_stage(expr: Expr) -> crate::Result<PipelineStage> {
    match expr {
        Expr::FunctionCall(name, args) => {
            let name_str = name.to_string();
            match name_str.as_str() {
                "read" => {
                    let path = extract_path_from_args("read", &args)?;
                    Ok(PipelineStage::Read { path })
                }
                "select" => {
                    let columns = extract_column_specs(&args)?;
                    if columns.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "select expects at least one column name".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Select { columns })
                }
                "head" => {
                    let n = extract_head_n(&args)?;
                    Ok(PipelineStage::Head { n })
                }
                "tail" => {
                    let n = extract_tail_n(&args)?;
                    Ok(PipelineStage::Tail { n })
                }
                "sample" => {
                    let n = extract_sample_n(&args)?;
                    Ok(PipelineStage::Sample { n })
                }
                "count" => {
                    let path = match args.as_slice() {
                        [] => None,
                        [Expr::Literal(Literal::String(s))] => Some(s.clone()),
                        _ => {
                            return Err(Error::UnsupportedFunctionCall(
                                "count takes no arguments or a single path string".to_string(),
                            ));
                        }
                    };
                    Ok(PipelineStage::Count { path })
                }
                "schema" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "schema takes no arguments".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Schema)
                }
                "write" => {
                    let path = extract_path_from_args("write", &args)?;
                    Ok(PipelineStage::Write { path })
                }
                _ => Err(Error::UnsupportedFunctionCall(name_str)),
            }
        }
        _ => Err(Error::UnsupportedExpression(expr.to_string())),
    }
}

/// Plans a full pipeline and returns whether the statement is incomplete
/// (the final explicit stage is non-terminal).
fn plan_pipeline_with_state(exprs: Vec<Expr>) -> crate::Result<(Vec<PipelineStage>, bool)> {
    let mut stages: Vec<PipelineStage> = exprs
        .into_iter()
        .map(plan_stage)
        .collect::<crate::Result<Vec<_>>>()?;
    optimize_read_then_count(&mut stages);
    let statement_incomplete = stages.last().is_some_and(PipelineStage::is_non_terminal);
    if let Some(implicit_stage) = stages
        .last()
        .and_then(PipelineStage::implicit_followup_stage)
    {
        stages.push(implicit_stage);
    }
    Ok((stages, statement_incomplete))
}

/// Replaces [Read { path }, Count { path: None }] with [Count { path: Some(path) }]
/// so Parquet/ORC use metadata without loading the file.
fn optimize_read_then_count(stages: &mut Vec<PipelineStage>) {
    if let [
        PipelineStage::Read { path },
        PipelineStage::Count { path: None },
    ] = stages.as_slice()
    {
        *stages = vec![PipelineStage::Count {
            path: Some(path.clone()),
        }];
    }
}

/// Extracts a single positive integer argument from a function call's args.
fn extract_usize_arg(func_name: &str, args: &[Expr]) -> crate::Result<usize> {
    match args {
        [Expr::Literal(Literal::Number(num))] => {
            let s = num.to_string();
            s.parse::<usize>().map_err(|_| {
                Error::UnsupportedFunctionCall(format!(
                    "{func_name} expects a positive integer argument, got {s}"
                ))
            })
        }
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "{func_name} expects a single integer argument, got {args:?}"
        ))),
    }
}

/// Extracts the integer argument N from a head() call's args.
fn extract_head_n(args: &[Expr]) -> crate::Result<usize> {
    extract_usize_arg("head", args)
}

/// Extracts the integer argument N from a tail() call's args.
fn extract_tail_n(args: &[Expr]) -> crate::Result<usize> {
    extract_usize_arg("tail", args)
}

/// Extracts the optional integer argument N from a sample() call's args (default 10).
fn extract_sample_n(args: &[Expr]) -> crate::Result<usize> {
    if args.is_empty() {
        return Ok(10);
    }
    extract_usize_arg("sample", args)
}

#[cfg(test)]
mod tests {
    use flt::ast::Identifier;
    use flt::parser::parse_expr;
    use tempfile::NamedTempFile;

    use super::*;

    fn new_context() -> ReplPipeline {
        ReplPipeline::new()
    }

    fn parse(input: &str) -> Expr {
        let (remainder, expr) = parse_expr(input).expect("parse");
        assert!(remainder.trim().is_empty(), "unconsumed: {remainder:?}");
        expr
    }

    // ── plan_stage ─────────────────────────────────────────────────

    #[test]
    fn test_plan_stage_read() {
        let expr = parse(r#"read("file.parquet")"#);
        let stage = plan_stage(expr).unwrap();
        assert_eq!(
            stage,
            PipelineStage::Read {
                path: "file.parquet".to_string()
            }
        );
    }

    #[test]
    fn test_plan_stage_select() {
        let expr = Expr::FunctionCall(
            Identifier("select".into()),
            vec![
                Expr::Literal(Literal::Symbol("one".into())),
                Expr::Literal(Literal::Symbol("two".into())),
            ],
        );
        let stage = plan_stage(expr).unwrap();
        assert_eq!(
            stage,
            PipelineStage::Select {
                columns: vec![
                    ColumnSpec::CaseInsensitive("one".into()),
                    ColumnSpec::CaseInsensitive("two".into())
                ]
            }
        );
    }

    #[test]
    fn test_plan_stage_head() {
        let expr = parse("head(5)");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Head { n: 5 });
    }

    #[test]
    fn test_plan_stage_write() {
        let expr = parse(r#"write("output.csv")"#);
        let stage = plan_stage(expr).unwrap();
        assert_eq!(
            stage,
            PipelineStage::Write {
                path: "output.csv".to_string()
            }
        );
    }

    #[test]
    fn test_plan_stage_unsupported_function() {
        let expr = Expr::FunctionCall(Identifier("unknown".into()), vec![]);
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_plan_stage_non_function_expr() {
        let expr = Expr::Ident("x".into());
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedExpression(_)
        ));
    }

    // ── plan_pipeline_with_state ──────────────────────────────────

    #[test]
    fn test_plan_pipeline_read_select_write() {
        let expr = parse(r#"read("a.parquet") |> select(:x) |> write("b.csv")"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(
            pipeline[1],
            PipelineStage::Select {
                columns: vec![ColumnSpec::CaseInsensitive("x".into())]
            }
        );
        assert_eq!(
            pipeline[2],
            PipelineStage::Write {
                path: "b.csv".to_string()
            }
        );
    }

    #[test]
    fn test_plan_pipeline_auto_appends_print_after_head() {
        let expr = parse(r#"read("a.parquet") |> head(5)"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(pipeline[1], PipelineStage::Head { n: 5 });
        assert_eq!(pipeline[2], PipelineStage::Print);
    }

    #[test]
    fn test_plan_pipeline_no_print_when_write_follows_head() {
        let expr = parse(r#"read("a.parquet") |> head(5) |> write("b.csv")"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert!(matches!(pipeline.last(), Some(PipelineStage::Write { .. })));
    }

    // ── extract_head_n ────────────────────────────────────────────

    #[test]
    fn test_extract_head_n_valid() {
        let args = parse_fn_args("head(10)");
        assert_eq!(extract_head_n(&args).unwrap(), 10);
    }

    #[test]
    fn test_extract_head_n_bad_args() {
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        assert!(extract_head_n(&args).is_err());
    }

    #[test]
    fn test_extract_head_n_empty_args() {
        assert!(extract_head_n(&[]).is_err());
    }

    // ── collect_pipe_stages ─────────────────────────────────────────

    #[test]
    fn test_collect_pipe_stages_single_expr() {
        let expr = Expr::Ident("a".into());
        let mut stages = Vec::new();
        collect_pipe_stages(expr, &mut stages);
        assert_eq!(stages.len(), 1);
        assert!(matches!(&stages[0], Expr::Ident(s) if s == "a"));
    }

    #[test]
    fn test_collect_pipe_stages_two_stages() {
        let expr = Expr::BinaryExpr(
            Box::new(Expr::Ident("a".into())),
            BinaryOp::Pipe,
            Box::new(Expr::Ident("b".into())),
        );
        let mut stages = Vec::new();
        collect_pipe_stages(expr, &mut stages);
        assert_eq!(stages.len(), 2);
        assert!(matches!(&stages[0], Expr::Ident(s) if s == "a"));
        assert!(matches!(&stages[1], Expr::Ident(s) if s == "b"));
    }

    #[test]
    fn test_collect_pipe_stages_three_stages() {
        let expr = parse(r#"read("a.parquet") |> select(:x) |> write("b.csv")"#);
        let mut stages = Vec::new();
        collect_pipe_stages(expr, &mut stages);
        assert_eq!(stages.len(), 3);
    }

    #[test]
    fn test_collect_pipe_stages_non_pipe_binary_not_flattened() {
        let expr = Expr::BinaryExpr(
            Box::new(Expr::Ident("a".into())),
            BinaryOp::Add,
            Box::new(Expr::Ident("b".into())),
        );
        let mut stages = Vec::new();
        collect_pipe_stages(expr, &mut stages);
        assert_eq!(stages.len(), 1);
        assert!(matches!(&stages[0], Expr::BinaryExpr(_, BinaryOp::Add, _)));
    }

    // ── extract_column_specs ────────────────────────────────────────

    #[test]
    fn test_extract_column_specs_symbols() {
        let args = vec![
            Expr::Literal(Literal::Symbol("one".into())),
            Expr::Literal(Literal::Symbol("two".into())),
        ];
        let result = extract_column_specs(&args).unwrap();
        assert_eq!(
            result,
            vec![
                ColumnSpec::CaseInsensitive("one".into()),
                ColumnSpec::CaseInsensitive("two".into())
            ]
        );
    }

    #[test]
    fn test_extract_column_specs_strings() {
        let args = vec![
            Expr::Literal(Literal::String("col_a".into())),
            Expr::Literal(Literal::String("col_b".into())),
        ];
        let result = extract_column_specs(&args).unwrap();
        assert_eq!(
            result,
            vec![
                ColumnSpec::Exact("col_a".into()),
                ColumnSpec::Exact("col_b".into())
            ]
        );
    }

    #[test]
    fn test_extract_column_specs_idents() {
        let args = vec![Expr::Ident("foo".into()), Expr::Ident("bar".into())];
        let result = extract_column_specs(&args).unwrap();
        assert_eq!(
            result,
            vec![
                ColumnSpec::CaseInsensitive("foo".into()),
                ColumnSpec::CaseInsensitive("bar".into())
            ]
        );
    }

    #[test]
    fn test_extract_column_specs_mixed() {
        let args = vec![
            Expr::Literal(Literal::Symbol("sym".into())),
            Expr::Literal(Literal::String("str".into())),
            Expr::Ident("ident".into()),
        ];
        let result = extract_column_specs(&args).unwrap();
        assert_eq!(
            result,
            vec![
                ColumnSpec::CaseInsensitive("sym".into()),
                ColumnSpec::Exact("str".into()),
                ColumnSpec::CaseInsensitive("ident".into())
            ]
        );
    }

    #[test]
    fn test_extract_column_specs_empty() {
        let result = extract_column_specs(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_column_specs_unsupported_expr() {
        let args = vec![Expr::Literal(Literal::Boolean(true))];
        let result = extract_column_specs(&args);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::UnsupportedFunctionCall(_)),
            "expected UnsupportedFunctionCall, got {err:?}"
        );
    }

    // ── eval: error paths ───────────────────────────────────────────

    #[test]
    fn test_eval_unsupported_expression() {
        let mut ctx = new_context();
        let expr = Expr::Literal(Literal::Boolean(true));
        let result = ctx.eval(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedExpression(_)
        ));
    }

    #[test]
    fn test_eval_unsupported_binary_operator() {
        let mut ctx = new_context();
        let expr = Expr::BinaryExpr(
            Box::new(Expr::Ident("a".into())),
            BinaryOp::Add,
            Box::new(Expr::Ident("b".into())),
        );
        let result = ctx.eval(expr);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::UnsupportedOperator(_)));
    }

    #[test]
    fn test_eval_sets_statement_incomplete_for_non_terminal_final_stage() {
        let mut ctx = new_context();
        let expr = parse(r#"read("fixtures/table.parquet")"#);
        let _ = ctx.eval(expr).expect("eval");
        assert!(ctx.statement_incomplete);
    }

    #[test]
    fn test_eval_clears_statement_incomplete_for_terminal_final_stage() {
        let mut ctx = new_context();
        let expr = parse(r#"read("fixtures/table.parquet") |> head(1)"#);
        let _ = ctx.eval(expr).expect("eval");
        assert!(!ctx.statement_incomplete);
    }

    #[test]
    fn test_eval_clears_statement_incomplete_for_write_final_stage() {
        let mut ctx = new_context();
        let expr = parse(r#"read("fixtures/table.parquet") |> write("out.csv")"#);
        let _ = ctx.eval(expr).expect("eval");
        assert!(!ctx.statement_incomplete);
    }

    #[test]
    fn test_eval_incremental_non_terminal_accumulates_state() {
        let mut ctx = new_context();
        let expr = parse(r#"read("fixtures/table.parquet")"#);
        let pipeline = ctx.eval_incremental(expr).expect("eval_incremental");
        assert!(pipeline.is_none());
        assert!(ctx.statement_incomplete);
        assert_eq!(ctx.pending_exprs.len(), 1);
    }

    #[test]
    fn test_eval_incremental_terminal_flushes_accumulated_pipeline() {
        let mut ctx = new_context();
        let first = parse(r#"read("fixtures/table.parquet")"#);
        let second = parse(r#"head(2)"#);

        let first_pipeline = ctx.eval_incremental(first).expect("first eval_incremental");
        assert!(first_pipeline.is_none());
        assert!(ctx.statement_incomplete);

        let second_pipeline = ctx
            .eval_incremental(second)
            .expect("second eval_incremental")
            .expect("pipeline should be complete");
        assert_eq!(second_pipeline.len(), 3);
        assert!(matches!(second_pipeline[0], PipelineStage::Read { .. }));
        assert_eq!(second_pipeline[1], PipelineStage::Head { n: 2 });
        assert_eq!(second_pipeline[2], PipelineStage::Print);
        assert!(!ctx.statement_incomplete);
        assert!(ctx.pending_exprs.is_empty());
    }

    #[test]
    fn test_eval_incremental_terminal_single_input_executes_immediately() {
        let mut ctx = new_context();
        let expr = parse(r#"read("fixtures/table.parquet") |> write("out.csv")"#);
        let pipeline = ctx
            .eval_incremental(expr)
            .expect("eval_incremental")
            .expect("pipeline should be complete");
        assert_eq!(pipeline.len(), 2);
        assert!(matches!(pipeline[0], PipelineStage::Read { .. }));
        assert!(matches!(pipeline[1], PipelineStage::Write { .. }));
        assert!(!ctx.statement_incomplete);
        assert!(ctx.pending_exprs.is_empty());
    }

    // ── exec_read / extract_path_from_args ──────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_read_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet")
            .await
            .expect("exec_read");
        assert!(ctx.batches.is_some());
        assert!(!ctx.batches.as_ref().unwrap().is_empty());
    }

    #[test]
    fn test_extract_path_from_args_read_bad_args() {
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = extract_path_from_args("read", &args);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_extract_path_from_args_read_no_args() {
        let result = extract_path_from_args("read", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_path_from_args_read_too_many_args() {
        let args = vec![
            Expr::Literal(Literal::String("a.parquet".into())),
            Expr::Literal(Literal::String("b.parquet".into())),
        ];
        let result = extract_path_from_args("read", &args);
        assert!(result.is_err());
    }

    // ── exec_select ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_select_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let columns = vec![
            ColumnSpec::CaseInsensitive("one".into()),
            ColumnSpec::CaseInsensitive("two".into()),
        ];
        ctx.exec_select(&columns).await.expect("select");
        let batches = ctx.batches.as_ref().expect("batches after select");
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(col_names, vec!["one", "two"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_select_no_preceding_read() {
        let mut ctx = new_context();
        let columns = vec![ColumnSpec::CaseInsensitive("one".into())];
        let result = ctx.exec_select(&columns).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[test]
    fn test_plan_stage_select_empty_columns_rejected() {
        let expr = parse("select()");
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── exec_write ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_write_success() {
        let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
        let output_path = tempfile.path().to_str().expect("Failed to get path");

        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_write(output_path).await.expect("write");

        assert!(tempfile.path().exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_path));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_write_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_write("out.csv").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[test]
    fn test_extract_path_from_args_write_bad_args() {
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = extract_path_from_args("write", &args);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── eval: full pipeline integration ─────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_write() {
        let tempfile = NamedTempFile::with_suffix(".avro").expect("Failed to create temp file");
        let output_path = tempfile.path().to_str().expect("Failed to get path");

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> write("{}")"#,
            &output_path
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_write_without_select() {
        let tempfile = NamedTempFile::with_suffix(".csv").expect("Failed to create temp file");
        let output_path = tempfile.path().to_str().expect("Failed to get path");

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> write("{}")"#,
            &output_path
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_path));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_with_strings() {
        let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
        let output_path = tempfile.path().to_str().expect("Failed to get path");

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select("one", "three") |> write("{}")"#,
            &output_path
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
        let batches = ctx.batches.as_ref();
        assert!(batches.is_none(), "batches consumed by write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_select_symbol_case_insensitive() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let columns = vec![
            ColumnSpec::CaseInsensitive("ONE".into()),
            ColumnSpec::CaseInsensitive("TWO".into()),
        ];
        ctx.exec_select(&columns).await.expect("select");
        let batches = ctx.batches.as_ref().expect("batches after select");
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(col_names, vec!["one", "two"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_select_string_exact_match_fails_on_wrong_case() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let columns = vec![ColumnSpec::Exact("ONE".into())];
        let result = ctx.exec_select(&columns).await;
        assert!(result.is_err());
    }

    // ── eval_pipe ───────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_pipe_single_read() {
        let expr = parse(r#"read("fixtures/table.parquet") |> select(:one)"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        let batches = ctx.batches.as_ref().expect("batches");
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "one");
    }

    // ── is_head_call ───────────────────────────────────────────────

    #[test]
    fn test_is_head_call_true() {
        let expr = parse("head(5)");
        assert!(is_head_call(Some(&expr)));
    }

    #[test]
    fn test_is_head_call_false_for_other_function() {
        let expr = parse(r#"read("file.csv")"#);
        assert!(!is_head_call(Some(&expr)));
    }

    #[test]
    fn test_is_head_call_false_for_none() {
        assert!(!is_head_call(None));
    }

    // ── exec_head ──────────────────────────────────────────────────

    fn parse_fn_args(input: &str) -> Vec<Expr> {
        match parse(input) {
            Expr::FunctionCall(_, args) => args,
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_head_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_head(2).expect("head");

        let batches = ctx.batches.as_ref().expect("batches after head");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_head_preserves_schema() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        ctx.exec_head(1).expect("head");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_exec_head_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_head(5);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── exec_head: pipeline integration ──────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_head_write() {
        let tempfile = NamedTempFile::with_suffix(".csv").expect("Failed to create temp file");
        let output_path = tempfile.path().to_string_lossy().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> head(2) |> write("{}")"#,
            &output_path.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_path.as_str()));
        assert!(ctx.batches.is_none(), "batches consumed by write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_head_write() {
        let tempfile = NamedTempFile::with_suffix(".csv").expect("Failed to create temp file");
        let output_path = tempfile.path().to_string_lossy().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> head(1) |> write("{}")"#,
            &output_path.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
    }

    // ── plan_stage: tail ─────────────────────────────────────────

    #[test]
    fn test_plan_stage_tail() {
        let expr = parse("tail(5)");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Tail { n: 5 });
    }

    #[test]
    fn test_terminal_stage_classification() {
        assert!(PipelineStage::Head { n: 1 }.is_terminal());
        assert!(PipelineStage::Tail { n: 1 }.is_terminal());
        assert!(
            PipelineStage::Write {
                path: "out.csv".into()
            }
            .is_terminal()
        );
        assert!(
            !PipelineStage::Select {
                columns: vec![ColumnSpec::CaseInsensitive("x".into())]
            }
            .is_terminal()
        );
        assert!(
            PipelineStage::Select {
                columns: vec![ColumnSpec::CaseInsensitive("x".into())]
            }
            .is_non_terminal()
        );
    }

    #[test]
    fn test_terminal_stage_implicit_followup() {
        assert_eq!(
            PipelineStage::Head { n: 5 }.implicit_followup_stage(),
            Some(PipelineStage::Print)
        );
        assert_eq!(
            PipelineStage::Tail { n: 5 }.implicit_followup_stage(),
            Some(PipelineStage::Print)
        );
        assert_eq!(
            PipelineStage::Write {
                path: "out.csv".into()
            }
            .implicit_followup_stage(),
            None
        );
        assert_eq!(
            PipelineStage::Count { path: None }.implicit_followup_stage(),
            None
        );
    }

    #[test]
    fn test_display_print_stage() {
        assert_eq!(PipelineStage::Print.to_string(), "print()");
    }

    // ── plan_pipeline_with_state: tail auto-print ─────────────────

    #[test]
    fn test_plan_pipeline_auto_appends_print_after_tail() {
        let expr = parse(r#"read("a.parquet") |> tail(5)"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(pipeline[1], PipelineStage::Tail { n: 5 });
        assert_eq!(pipeline[2], PipelineStage::Print);
    }

    #[test]
    fn test_plan_pipeline_no_print_when_write_follows_tail() {
        let expr = parse(r#"read("a.parquet") |> tail(5) |> write("b.csv")"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert!(matches!(pipeline.last(), Some(PipelineStage::Write { .. })));
    }

    // ── extract_tail_n ──────────────────────────────────────────

    #[test]
    fn test_extract_tail_n_valid() {
        let args = parse_fn_args("tail(10)");
        assert_eq!(extract_tail_n(&args).unwrap(), 10);
    }

    #[test]
    fn test_extract_tail_n_bad_args() {
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        assert!(extract_tail_n(&args).is_err());
    }

    #[test]
    fn test_extract_tail_n_empty_args() {
        assert!(extract_tail_n(&[]).is_err());
    }

    // ── exec_tail ───────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_tail_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_tail(2).expect("tail");

        let batches = ctx.batches.as_ref().expect("batches after tail");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_tail_preserves_schema() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        ctx.exec_tail(1).expect("tail");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_exec_tail_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_tail(5);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── exec_tail: pipeline integration ──────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_tail_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("tailed.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> tail(2) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
        assert!(ctx.batches.is_none(), "batches consumed by write");
    }

    // ── plan_stage: count ────────────────────────────────────────

    #[test]
    fn test_plan_stage_count() {
        let expr = parse("count()");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Count { path: None });
    }

    #[test]
    fn test_plan_stage_count_with_path() {
        let expr = parse(r#"count("fixtures/table.parquet")"#);
        let stage = plan_stage(expr).unwrap();
        assert_eq!(
            stage,
            PipelineStage::Count {
                path: Some("fixtures/table.parquet".to_string())
            }
        );
    }

    #[test]
    fn test_plan_stage_count_rejects_invalid_args() {
        let expr = Expr::FunctionCall(
            Identifier("count".into()),
            vec![
                Expr::Literal(Literal::String("a".into())),
                Expr::Literal(Literal::String("b".into())),
            ],
        );
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── plan_pipeline_with_state: count does not auto-append print

    #[test]
    fn test_plan_pipeline_count_no_auto_print() {
        let expr = parse(r#"read("a.parquet") |> count()"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        // read(path) |> count() is optimized to count(path) so Parquet/ORC use metadata
        assert_eq!(pipeline.len(), 1);
        assert_eq!(
            pipeline[0],
            PipelineStage::Count {
                path: Some("a.parquet".to_string())
            }
        );
    }

    #[test]
    fn test_plan_pipeline_read_select_count_not_optimized() {
        let expr = parse(r#"read("a.parquet") |> select(:x) |> count()"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert!(matches!(&pipeline[1], PipelineStage::Select { .. }));
        assert_eq!(pipeline[2], PipelineStage::Count { path: None });
    }

    // ── exec_count ─────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_count_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_count().expect("count");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[test]
    fn test_exec_count_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_count();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── exec_count: pipeline integration ────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> count()"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> select(:one, :two) |> count()"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_head_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> head(2) |> count()"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_count_path_uses_efficient_path() {
        let expr = parse(r#"count("fixtures/table.parquet")"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "count(path) does not use batches");
    }

    // ── plan_stage: schema ────────────────────────────────────────

    #[test]
    fn test_plan_stage_schema() {
        let expr = parse("schema()");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Schema);
    }

    #[test]
    fn test_plan_stage_schema_rejects_args() {
        let expr = Expr::FunctionCall(
            Identifier("schema".into()),
            vec![Expr::Literal(Literal::String("extra".into()))],
        );
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_schema_is_terminal() {
        assert!(PipelineStage::Schema.is_terminal());
    }

    #[test]
    fn test_schema_no_implicit_followup() {
        assert_eq!(PipelineStage::Schema.implicit_followup_stage(), None);
    }

    #[test]
    fn test_display_schema_stage() {
        assert_eq!(PipelineStage::Schema.to_string(), "schema()");
    }

    // ── plan_pipeline_with_state: schema does not auto-append print

    #[test]
    fn test_plan_pipeline_schema_no_auto_print() {
        let expr = parse(r#"read("a.parquet") |> schema()"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 2);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(pipeline[1], PipelineStage::Schema);
    }

    // ── exec_schema ─────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_schema_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_schema().expect("schema");
        assert!(ctx.batches.is_none(), "batches consumed by schema");
    }

    #[test]
    fn test_exec_schema_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_schema();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── exec_schema: pipeline integration ────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_schema() {
        let expr = parse(r#"read("fixtures/table.parquet") |> schema()"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "batches consumed by schema");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_schema() {
        let expr = parse(r#"read("fixtures/table.parquet") |> select(:one, :two) |> schema()"#);
        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");
        assert!(ctx.batches.is_none(), "batches consumed by schema");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_tail_write() {
        let tempfile = NamedTempFile::with_suffix(".csv").expect("Failed to create temp file");
        let output_path = tempfile.path().to_string_lossy().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> tail(1) |> write("{}")"#,
            &output_path.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(tempfile.path().exists());
    }

    // ── plan_stage: sample ──────────────────────────────────────

    #[test]
    fn test_plan_stage_sample_with_arg() {
        let expr = parse("sample(5)");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Sample { n: 5 });
    }

    #[test]
    fn test_plan_stage_sample_no_arg_defaults_to_10() {
        let expr = Expr::FunctionCall(Identifier("sample".into()), vec![]);
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Sample { n: 10 });
    }

    #[test]
    fn test_sample_is_terminal() {
        assert!(PipelineStage::Sample { n: 5 }.is_terminal());
    }

    #[test]
    fn test_sample_implicit_followup_is_print() {
        assert_eq!(
            PipelineStage::Sample { n: 5 }.implicit_followup_stage(),
            Some(PipelineStage::Print)
        );
    }

    // ── plan_pipeline_with_state: sample auto-print ───────────────

    #[test]
    fn test_plan_pipeline_auto_appends_print_after_sample() {
        let expr = parse(r#"read("a.parquet") |> sample(5)"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(pipeline[1], PipelineStage::Sample { n: 5 });
        assert_eq!(pipeline[2], PipelineStage::Print);
    }

    #[test]
    fn test_plan_pipeline_no_print_when_write_follows_sample() {
        let expr = parse(r#"read("a.parquet") |> sample(5) |> write("b.csv")"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
        assert_eq!(pipeline.len(), 3);
        assert!(matches!(pipeline.last(), Some(PipelineStage::Write { .. })));
    }

    // ── extract_sample_n ────────────────────────────────────────

    #[test]
    fn test_extract_sample_n_valid() {
        let args = parse_fn_args("sample(10)");
        assert_eq!(extract_sample_n(&args).unwrap(), 10);
    }

    #[test]
    fn test_extract_sample_n_empty_defaults_to_10() {
        assert_eq!(extract_sample_n(&[]).unwrap(), 10);
    }

    #[test]
    fn test_extract_sample_n_bad_args() {
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        assert!(extract_sample_n(&args).is_err());
    }

    // ── exec_sample ─────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_sample_success() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_sample(2).expect("sample");

        let batches = ctx.batches.as_ref().expect("batches after sample");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_sample_default_n() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        ctx.exec_sample(10).expect("sample with n=10");

        let batches = ctx.batches.as_ref().expect("batches after sample");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec_sample_preserves_schema() {
        let mut ctx = new_context();
        ctx.exec_read("fixtures/table.parquet").await.expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        ctx.exec_sample(1).expect("sample");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_exec_sample_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.exec_sample(5);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── exec_sample: pipeline integration ────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_sample_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("sampled.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> sample(2) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
        assert!(ctx.batches.is_none(), "batches consumed by write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_sample_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("selected_sampled.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> sample(1) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        let pipeline_stages = ctx.eval(expr).expect("eval");
        ctx.execute_pipeline(pipeline_stages)
            .await
            .expect("execute");

        assert!(output_path.exists());
    }

    #[test]
    fn test_display_sample_stage() {
        assert_eq!(PipelineStage::Sample { n: 10 }.to_string(), "sample(10)");
    }
}
