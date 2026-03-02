use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;

use crate::Error;
use crate::FileType;
use crate::pipeline::Source;
use crate::pipeline::VecRecordBatchReaderSource;
use crate::pipeline::display::write_record_batches_as_csv;
use crate::pipeline::read_to_batches;
use crate::pipeline::select;
use crate::pipeline::write_batches;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
enum PipelineStage {
    Read { path: String },
    Select { columns: Vec<String> },
    Head { n: usize },
    Tail { n: usize },
    Count,
    Write { path: String },
    Print,
}

/// Builder for a REPL pipeline.
pub struct ReplPipelineBuilder {
    pub batches: Option<Vec<arrow::record_batch::RecordBatch>>,
    pub writer: Option<String>,
}

impl Default for ReplPipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplPipelineBuilder {
    pub fn new() -> Self {
        Self {
            batches: None,
            writer: None,
        }
    }

    /// Evaluates a datu REPL expression.
    pub async fn eval(&mut self, expr: Expr) -> crate::Result<()> {
        match expr {
            Expr::BinaryExpr(left, op, right) => {
                self.eval_binary_expr(left, op, right).await?;
            }
            Expr::FunctionCall(name, args) => {
                let pipeline = plan_pipeline(vec![Expr::FunctionCall(name, args)])?;
                self.execute_pipeline(pipeline).await?;
            }
            _ => return Err(Error::UnsupportedExpression(expr.to_string())),
        }
        Ok(())
    }

    /// Evaluates a binary expression.
    async fn eval_binary_expr(
        &mut self,
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    ) -> crate::Result<()> {
        match op {
            BinaryOp::Pipe => {
                self.eval_pipe(left, right).await?;
            }
            _ => return Err(Error::UnsupportedOperator(op.to_string())),
        }
        Ok(())
    }

    /// Plans and executes a pipe expression as a pipeline.
    async fn eval_pipe(&mut self, left: Box<Expr>, right: Box<Expr>) -> crate::Result<()> {
        let mut exprs = Vec::new();
        collect_pipe_stages(*left, &mut exprs);
        collect_pipe_stages(*right, &mut exprs);
        let pipeline = plan_pipeline(exprs)?;
        self.execute_pipeline(pipeline).await
    }

    /// Executes a planned pipeline.
    async fn execute_pipeline(&mut self, stages: Vec<PipelineStage>) -> crate::Result<()> {
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
            PipelineStage::Count => self.exec_count(),
            PipelineStage::Write { path } => self.exec_write(&path).await,
            PipelineStage::Print => self.print_batches(),
        }
    }

    /// Reads a file into record batches.
    async fn exec_read(&mut self, path: &str) -> crate::Result<()> {
        let file_type: FileType = path.try_into()?;
        let batches = read_to_batches(path, file_type, &None, None)
            .await
            .map_err(|e| Error::GenericError(e.to_string()))?;
        self.batches = Some(batches);
        Ok(())
    }

    /// Selects columns from the batches in context.
    async fn exec_select(&mut self, columns: &[String]) -> crate::Result<()> {
        let batches = self.batches.take().ok_or_else(|| {
            Error::GenericError("select requires a preceding read in the pipe".to_string())
        })?;
        let selected = select::select_columns_to_batches(batches, columns).await?;
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
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let number = n.min(total_rows);
        let skip = total_rows.saturating_sub(number);

        let mut result = Vec::new();
        let mut rows_emitted = 0usize;
        let mut rows_skipped = 0usize;
        for batch in batches {
            let batch_rows = batch.num_rows();
            if rows_skipped + batch_rows <= skip {
                rows_skipped += batch_rows;
                continue;
            }
            let start_in_batch = skip.saturating_sub(rows_skipped);
            rows_skipped += start_in_batch;
            let take = (number - rows_emitted).min(batch_rows - start_in_batch);
            if take == 0 {
                break;
            }
            result.push(batch.slice(start_in_batch, take));
            rows_emitted += take;
        }
        self.batches = Some(result);
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
        write_record_batches_as_csv(&mut *reader, std::io::stdout())
    }
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

/// Extracts a single path string from a function's argument list.
fn extract_path_from_args(func_name: &str, args: &[Expr]) -> crate::Result<String> {
    match args {
        [Expr::Literal(Literal::String(s))] => Ok(s.clone()),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "{func_name} expects a single string argument, got {args:?}"
        ))),
    }
}

/// Extracts column names from select args (symbols like :one, identifiers, or strings like "one").
fn extract_column_names(args: &[Expr]) -> crate::Result<Vec<String>> {
    args.iter()
        .map(|expr| match expr {
            Expr::Literal(Literal::Symbol(s)) => Ok(s.clone()),
            Expr::Literal(Literal::String(s)) => Ok(s.clone()),
            Expr::Ident(s) => Ok(s.clone()),
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "select expects symbol or string column names, got {expr:?}"
            ))),
        })
        .collect()
}

#[cfg(test)]
impl ReplPipelineBuilder {
    async fn eval_stage(&mut self, expr: Expr) -> crate::Result<()> {
        let stage = plan_stage(expr)?;
        self.execute_stage(stage).await
    }

    async fn eval_read(&mut self, args: Vec<Expr>) -> crate::Result<()> {
        let path = extract_path_from_args("read", &args)?;
        self.exec_read(&path).await
    }

    async fn eval_select(&mut self, args: Vec<Expr>) -> crate::Result<()> {
        let columns = extract_column_names(&args)?;
        if columns.is_empty() {
            return Err(Error::UnsupportedFunctionCall(
                "select expects at least one column name".to_string(),
            ));
        }
        self.exec_select(&columns).await
    }

    fn eval_head(&mut self, args: Vec<Expr>) -> crate::Result<()> {
        let n = extract_head_n(&args)?;
        self.exec_head(n)
    }

    fn eval_tail(&mut self, args: Vec<Expr>) -> crate::Result<()> {
        let n = extract_tail_n(&args)?;
        self.exec_tail(n)
    }

    async fn eval_write(&mut self, args: Vec<Expr>) -> crate::Result<()> {
        let path = extract_path_from_args("write", &args)?;
        self.exec_write(&path).await
    }

    fn eval_count(&mut self) -> crate::Result<()> {
        self.exec_count()
    }
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
                    let columns = extract_column_names(&args)?;
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
                "count" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "count takes no arguments".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Count)
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

/// Plans a full pipeline from a list of AST expressions.
/// Automatically appends a Print stage if the last stage is `head()` or `tail()`
fn plan_pipeline(exprs: Vec<Expr>) -> crate::Result<Vec<PipelineStage>> {
    let mut stages: Vec<PipelineStage> = exprs
        .into_iter()
        .map(plan_stage)
        .collect::<crate::Result<Vec<_>>>()?;
    if matches!(
        stages.last(),
        Some(PipelineStage::Head { .. } | PipelineStage::Tail { .. })
    ) {
        stages.push(PipelineStage::Print);
    }
    Ok(stages)
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

#[cfg(test)]
mod tests {
    use flt::ast::Identifier;
    use flt::parser::parse_expr;

    use super::*;

    fn new_context() -> ReplPipelineBuilder {
        ReplPipelineBuilder::new()
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
                columns: vec!["one".to_string(), "two".to_string()]
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

    // ── plan_pipeline ─────────────────────────────────────────────

    #[test]
    fn test_plan_pipeline_read_select_write() {
        let expr = parse(r#"read("a.parquet") |> select(:x) |> write("b.csv")"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let pipeline = plan_pipeline(exprs).unwrap();
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
                columns: vec!["x".to_string()]
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
        let pipeline = plan_pipeline(exprs).unwrap();
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
        let pipeline = plan_pipeline(exprs).unwrap();
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

    // ── extract_column_names ────────────────────────────────────────

    #[test]
    fn test_extract_column_names_symbols() {
        let args = vec![
            Expr::Literal(Literal::Symbol("one".into())),
            Expr::Literal(Literal::Symbol("two".into())),
        ];
        let result = extract_column_names(&args).unwrap();
        assert_eq!(result, vec!["one", "two"]);
    }

    #[test]
    fn test_extract_column_names_strings() {
        let args = vec![
            Expr::Literal(Literal::String("col_a".into())),
            Expr::Literal(Literal::String("col_b".into())),
        ];
        let result = extract_column_names(&args).unwrap();
        assert_eq!(result, vec!["col_a", "col_b"]);
    }

    #[test]
    fn test_extract_column_names_idents() {
        let args = vec![Expr::Ident("foo".into()), Expr::Ident("bar".into())];
        let result = extract_column_names(&args).unwrap();
        assert_eq!(result, vec!["foo", "bar"]);
    }

    #[test]
    fn test_extract_column_names_mixed() {
        let args = vec![
            Expr::Literal(Literal::Symbol("sym".into())),
            Expr::Literal(Literal::String("str".into())),
            Expr::Ident("ident".into()),
        ];
        let result = extract_column_names(&args).unwrap();
        assert_eq!(result, vec!["sym", "str", "ident"]);
    }

    #[test]
    fn test_extract_column_names_empty() {
        let result = extract_column_names(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_column_names_unsupported_expr() {
        let args = vec![Expr::Literal(Literal::Boolean(true))];
        let result = extract_column_names(&args);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::UnsupportedFunctionCall(_)),
            "expected UnsupportedFunctionCall, got {err:?}"
        );
    }

    // ── eval: error paths ───────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_unsupported_expression() {
        let mut ctx = new_context();
        let expr = Expr::Literal(Literal::Boolean(true));
        let result = ctx.eval(expr).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedExpression(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_unsupported_binary_operator() {
        let mut ctx = new_context();
        let expr = Expr::BinaryExpr(
            Box::new(Expr::Ident("a".into())),
            BinaryOp::Add,
            Box::new(Expr::Ident("b".into())),
        );
        let result = ctx.eval(expr).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::UnsupportedOperator(_)));
    }

    // ── eval_stage: error paths ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_stage_unsupported_function() {
        let mut ctx = new_context();
        let expr = Expr::FunctionCall(Identifier("unknown".into()), vec![]);
        let result = ctx.eval_stage(expr).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_stage_non_function_expr() {
        let mut ctx = new_context();
        let expr = Expr::Ident("x".into());
        let result = ctx.eval_stage(expr).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedExpression(_)
        ));
    }

    // ── eval_read ───────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_success() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))];
        ctx.eval_read(args).await.expect("eval_read");
        assert!(ctx.batches.is_some());
        assert!(!ctx.batches.as_ref().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_bad_args() {
        let mut ctx = new_context();
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = ctx.eval_read(args).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_no_args() {
        let mut ctx = new_context();
        let result = ctx.eval_read(vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_too_many_args() {
        let mut ctx = new_context();
        let args = vec![
            Expr::Literal(Literal::String("a.parquet".into())),
            Expr::Literal(Literal::String("b.parquet".into())),
        ];
        let result = ctx.eval_read(args).await;
        assert!(result.is_err());
    }

    // ── eval_select ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_success() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let args = vec![
            Expr::Literal(Literal::Symbol("one".into())),
            Expr::Literal(Literal::Symbol("two".into())),
        ];
        ctx.eval_select(args).await.expect("select");
        let batches = ctx.batches.as_ref().expect("batches after select");
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(col_names, vec!["one", "two"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_no_preceding_read() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::Symbol("one".into()))];
        let result = ctx.eval_select(args).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_empty_columns() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let result = ctx.eval_select(vec![]).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── eval_write ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_write_success() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("output.parquet");
        let output_str = output_path.to_str().unwrap().to_string();

        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let args = vec![Expr::Literal(Literal::String(output_str.clone()))];
        ctx.eval_write(args).await.expect("write");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_write_no_preceding_read() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String("out.csv".into()))];
        let result = ctx.eval_write(args).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_write_bad_args() {
        let mut ctx = new_context();
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = ctx.eval_write(args).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── eval: full pipeline integration ─────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_write() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.avro");
        let output_str = output_path.to_str().expect("path to str").to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_write_without_select() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("table.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_with_strings() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("out.parquet");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select("one", "three") |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
        let batches = ctx.batches.as_ref();
        assert!(batches.is_none(), "batches consumed by write");
    }

    // ── eval_pipe ───────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_pipe_single_read() {
        let expr = parse(r#"read("fixtures/table.parquet")"#);
        if let Expr::FunctionCall(_, _) = &expr {
            let mut ctx = new_context();
            let left = Box::new(expr);
            let right = Box::new(Expr::FunctionCall(
                Identifier("select".into()),
                vec![Expr::Literal(Literal::Symbol("one".into()))],
            ));
            ctx.eval_pipe(left, right).await.expect("eval_pipe");
            let batches = ctx.batches.as_ref().expect("batches");
            let schema = batches[0].schema();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "one");
        } else {
            panic!("expected FunctionCall");
        }
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

    // ── eval_head ──────────────────────────────────────────────────

    fn parse_fn_args(input: &str) -> Vec<Expr> {
        match parse(input) {
            Expr::FunctionCall(_, args) => args,
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_head_success() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let args = parse_fn_args("head(2)");
        ctx.eval_head(args).expect("head");

        let batches = ctx.batches.as_ref().expect("batches after head");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_head_preserves_schema() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        let args = parse_fn_args("head(1)");
        ctx.eval_head(args).expect("head");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_eval_head_no_preceding_read() {
        let mut ctx = new_context();
        let args = parse_fn_args("head(5)");
        let result = ctx.eval_head(args);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[test]
    fn test_eval_head_bad_args_string() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        let result = ctx.eval_head(args);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_eval_head_no_args() {
        let mut ctx = new_context();
        let result = ctx.eval_head(vec![]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── eval_head: pipeline integration ────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_head_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("headed.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> head(2) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
        assert!(ctx.batches.is_none(), "batches consumed by write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_head_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("selected_headed.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> head(1) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
    }

    // ── plan_stage: tail ─────────────────────────────────────────

    #[test]
    fn test_plan_stage_tail() {
        let expr = parse("tail(5)");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Tail { n: 5 });
    }

    // ── plan_pipeline: tail auto-print ──────────────────────────

    #[test]
    fn test_plan_pipeline_auto_appends_print_after_tail() {
        let expr = parse(r#"read("a.parquet") |> tail(5)"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let pipeline = plan_pipeline(exprs).unwrap();
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
        let pipeline = plan_pipeline(exprs).unwrap();
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

    // ── eval_tail ───────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_tail_success() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let args = parse_fn_args("tail(2)");
        ctx.eval_tail(args).expect("tail");

        let batches = ctx.batches.as_ref().expect("batches after tail");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_tail_preserves_schema() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        let args = parse_fn_args("tail(1)");
        ctx.eval_tail(args).expect("tail");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_eval_tail_no_preceding_read() {
        let mut ctx = new_context();
        let args = parse_fn_args("tail(5)");
        let result = ctx.eval_tail(args);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[test]
    fn test_eval_tail_bad_args_string() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        let result = ctx.eval_tail(args);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_eval_tail_no_args() {
        let mut ctx = new_context();
        let result = ctx.eval_tail(vec![]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── eval_tail: pipeline integration ─────────────────────────

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
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
        assert!(ctx.batches.is_none(), "batches consumed by write");
    }

    // ── plan_stage: count ────────────────────────────────────────

    #[test]
    fn test_plan_stage_count() {
        let expr = parse("count()");
        let stage = plan_stage(expr).unwrap();
        assert_eq!(stage, PipelineStage::Count);
    }

    #[test]
    fn test_plan_stage_count_rejects_args() {
        let expr = Expr::FunctionCall(
            Identifier("count".into()),
            vec![Expr::Literal(Literal::String("extra".into()))],
        );
        let result = plan_stage(expr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    // ── plan_pipeline: count does not auto-append print ─────────

    #[test]
    fn test_plan_pipeline_count_no_auto_print() {
        let expr = parse(r#"read("a.parquet") |> count()"#);
        let mut exprs = Vec::new();
        collect_pipe_stages(expr, &mut exprs);
        let pipeline = plan_pipeline(exprs).unwrap();
        assert_eq!(pipeline.len(), 2);
        assert_eq!(
            pipeline[0],
            PipelineStage::Read {
                path: "a.parquet".to_string()
            }
        );
        assert_eq!(pipeline[1], PipelineStage::Count);
    }

    // ── eval_count ─────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_count_success() {
        let mut ctx = new_context();
        ctx.eval_read(vec![Expr::Literal(Literal::String(
            "fixtures/table.parquet".into(),
        ))])
        .await
        .expect("read");

        ctx.eval_count().expect("count");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[test]
    fn test_eval_count_no_preceding_read() {
        let mut ctx = new_context();
        let result = ctx.eval_count();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    // ── eval_count: pipeline integration ────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> count()"#);
        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> select(:one, :two) |> count()"#);
        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_head_count() {
        let expr = parse(r#"read("fixtures/table.parquet") |> head(2) |> count()"#);
        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");
        assert!(ctx.batches.is_none(), "batches consumed by count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repl_pipeline_read_select_tail_write() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let output_path = temp_dir.path().join("selected_tailed.csv");
        let output_str = output_path.to_str().unwrap().to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> tail(1) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let expr = parse(&pipeline);

        let mut ctx = new_context();
        ctx.eval(expr).await.expect("eval");

        assert!(output_path.exists());
    }
}
