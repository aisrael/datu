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

/// Context for a REPL session.
pub struct ReplContext {
    pub batches: Option<Vec<arrow::record_batch::RecordBatch>>,
    pub writer: Option<String>,
}

/// Evaluates a datu expression.
pub async fn eval(context: &mut ReplContext, expr: Expr) -> crate::Result<()> {
    match expr {
        Expr::BinaryExpr(left, op, right) => {
            eval_binary_expr(context, left, op, right).await?;
        }
        Expr::FunctionCall(name, args) => {
            let is_head = name == "head";
            eval_stage(context, Expr::FunctionCall(name, args)).await?;
            if is_head {
                print_batches(context)?;
            }
        }
        _ => return Err(Error::UnsupportedExpression(expr.to_string())),
    }
    Ok(())
}

/// Evaluates a binary expression.
async fn eval_binary_expr(
    context: &mut ReplContext,
    left: Box<Expr>,
    op: BinaryOp,
    right: Box<Expr>,
) -> crate::Result<()> {
    match op {
        BinaryOp::Pipe => {
            eval_pipe(context, left, right).await?;
        }
        _ => return Err(Error::UnsupportedOperator(op.to_string())),
    }
    Ok(())
}

/// Evaluates a pipe expression.
async fn eval_pipe(
    context: &mut ReplContext,
    left: Box<Expr>,
    right: Box<Expr>,
) -> crate::Result<()> {
    let mut stages = Vec::new();
    collect_pipe_stages(*left, &mut stages);
    collect_pipe_stages(*right, &mut stages);
    let needs_print = is_head_call(stages.last());
    for stage in stages {
        eval_stage(context, stage).await?;
    }
    if needs_print {
        print_batches(context)?;
    }
    Ok(())
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

/// Evaluates a single pipeline stage (read, select, head, or write).
async fn eval_stage(context: &mut ReplContext, expr: Expr) -> crate::Result<()> {
    match expr {
        Expr::FunctionCall(name, args) => {
            let name_str = name.to_string();
            match name_str.as_str() {
                "read" => eval_read(context, args).await?,
                "select" => eval_select(context, args).await?,
                "head" => eval_head(context, args)?,
                "write" => eval_write(context, args).await?,
                _ => return Err(Error::UnsupportedFunctionCall(name_str)),
            }
        }
        _ => return Err(Error::UnsupportedExpression(expr.to_string())),
    }
    Ok(())
}

/// Evaluates a read function call.
async fn eval_read(context: &mut ReplContext, args: Vec<Expr>) -> crate::Result<()> {
    let path = match args.as_slice() {
        [Expr::Literal(Literal::String(s))] => s.clone(),
        _ => {
            return Err(Error::UnsupportedFunctionCall(format!(
                "read expects a single string argument, got {args:?}"
            )));
        }
    };
    let file_type: FileType = path.as_str().try_into()?;
    let batches = read_to_batches(&path, file_type, &None, None)
        .await
        .map_err(|e| Error::GenericError(e.to_string()))?;
    context.batches = Some(batches);
    Ok(())
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

/// Evaluates a select function call using the DataFusion DataFrame API.
async fn eval_select(context: &mut ReplContext, args: Vec<Expr>) -> crate::Result<()> {
    let columns = extract_column_names(&args)?;
    if columns.is_empty() {
        return Err(Error::UnsupportedFunctionCall(
            "select expects at least one column name".to_string(),
        ));
    }
    let batches = context.batches.take().ok_or_else(|| {
        Error::GenericError("select requires a preceding read in the pipe".to_string())
    })?;
    let selected = select::select_columns_to_batches(batches, &columns).await?;
    context.batches = Some(selected);
    Ok(())
}

/// Evaluates a write function call.
async fn eval_write(context: &mut ReplContext, args: Vec<Expr>) -> crate::Result<()> {
    let output_path = match args.as_slice() {
        [Expr::Literal(Literal::String(s))] => s.clone(),
        _ => {
            return Err(Error::UnsupportedFunctionCall(format!(
                "write expects a single string argument, got {args:?}"
            )));
        }
    };
    let batches = context.batches.take().ok_or_else(|| {
        Error::GenericError("write requires a preceding read in the pipe".to_string())
    })?;
    let output_file_type: FileType = output_path.as_str().try_into()?;
    write_batches(batches, &output_path, output_file_type, true, false)
        .await
        .map_err(|e| Error::GenericError(e.to_string()))?;
    context.writer = Some(output_path);
    Ok(())
}

/// Returns true if the given expression is a `head(...)` function call.
fn is_head_call(expr: Option<&Expr>) -> bool {
    if let Some(Expr::FunctionCall(name, _)) = expr {
        *name == "head"
    } else {
        false
    }
}

/// Evaluates a head function call: takes the first N rows from the batches in context.
fn eval_head(context: &mut ReplContext, args: Vec<Expr>) -> crate::Result<()> {
    let n = match args.as_slice() {
        [Expr::Literal(Literal::Number(num))] => {
            let s = num.to_string();
            s.parse::<usize>().map_err(|_| {
                Error::UnsupportedFunctionCall(format!(
                    "head expects a positive integer argument, got {s}"
                ))
            })?
        }
        _ => {
            return Err(Error::UnsupportedFunctionCall(format!(
                "head expects a single integer argument, got {args:?}"
            )));
        }
    };
    let batches = context.batches.take().ok_or_else(|| {
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
    context.batches = Some(result);
    Ok(())
}

/// Prints batches from the context as CSV to stdout (implicit `print(:csv)`).
fn print_batches(context: &mut ReplContext) -> crate::Result<()> {
    let batches = context
        .batches
        .take()
        .ok_or_else(|| Error::GenericError("print requires batches in the context".to_string()))?;
    let mut source = VecRecordBatchReaderSource::new(batches);
    let mut reader = source.get()?;
    write_record_batches_as_csv(&mut *reader, std::io::stdout())
}

#[cfg(test)]
mod tests {
    use flt::ast::Identifier;
    use flt::parser::parse_expr;

    use super::*;

    fn new_context() -> ReplContext {
        ReplContext {
            batches: None,
            writer: None,
        }
    }

    fn parse(input: &str) -> Expr {
        let (remainder, expr) = parse_expr(input).expect("parse");
        assert!(remainder.trim().is_empty(), "unconsumed: {remainder:?}");
        expr
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
        let result = eval(&mut ctx, expr).await;
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
        let result = eval(&mut ctx, expr).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::UnsupportedOperator(_)));
    }

    // ── eval_stage: error paths ─────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_stage_unsupported_function() {
        let mut ctx = new_context();
        let expr = Expr::FunctionCall(Identifier("unknown".into()), vec![]);
        let result = eval_stage(&mut ctx, expr).await;
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
        let result = eval_stage(&mut ctx, expr).await;
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
        eval_read(&mut ctx, args).await.expect("eval_read");
        assert!(ctx.batches.is_some());
        assert!(!ctx.batches.as_ref().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_bad_args() {
        let mut ctx = new_context();
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = eval_read(&mut ctx, args).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_no_args() {
        let mut ctx = new_context();
        let result = eval_read(&mut ctx, vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_read_too_many_args() {
        let mut ctx = new_context();
        let args = vec![
            Expr::Literal(Literal::String("a.parquet".into())),
            Expr::Literal(Literal::String("b.parquet".into())),
        ];
        let result = eval_read(&mut ctx, args).await;
        assert!(result.is_err());
    }

    // ── eval_select ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_success() {
        let mut ctx = new_context();
        eval_read(
            &mut ctx,
            vec![Expr::Literal(Literal::String(
                "fixtures/table.parquet".into(),
            ))],
        )
        .await
        .expect("read");

        let args = vec![
            Expr::Literal(Literal::Symbol("one".into())),
            Expr::Literal(Literal::Symbol("two".into())),
        ];
        eval_select(&mut ctx, args).await.expect("select");
        let batches = ctx.batches.as_ref().expect("batches after select");
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(col_names, vec!["one", "two"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_no_preceding_read() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::Symbol("one".into()))];
        let result = eval_select(&mut ctx, args).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_select_empty_columns() {
        let mut ctx = new_context();
        eval_read(
            &mut ctx,
            vec![Expr::Literal(Literal::String(
                "fixtures/table.parquet".into(),
            ))],
        )
        .await
        .expect("read");

        let result = eval_select(&mut ctx, vec![]).await;
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
        eval_read(
            &mut ctx,
            vec![Expr::Literal(Literal::String(
                "fixtures/table.parquet".into(),
            ))],
        )
        .await
        .expect("read");

        let args = vec![Expr::Literal(Literal::String(output_str.clone()))];
        eval_write(&mut ctx, args).await.expect("write");

        assert!(output_path.exists());
        assert_eq!(ctx.writer.as_deref(), Some(output_str.as_str()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_write_no_preceding_read() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String("out.csv".into()))];
        let result = eval_write(&mut ctx, args).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_write_bad_args() {
        let mut ctx = new_context();
        let args = vec![Expr::Ident("not_a_string".into())];
        let result = eval_write(&mut ctx, args).await;
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
        eval(&mut ctx, expr).await.expect("eval");

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
        eval(&mut ctx, expr).await.expect("eval");

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
        eval(&mut ctx, expr).await.expect("eval");

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
            eval_pipe(&mut ctx, left, right).await.expect("eval_pipe");
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
        eval_read(
            &mut ctx,
            vec![Expr::Literal(Literal::String(
                "fixtures/table.parquet".into(),
            ))],
        )
        .await
        .expect("read");

        let args = parse_fn_args("head(2)");
        eval_head(&mut ctx, args).expect("head");

        let batches = ctx.batches.as_ref().expect("batches after head");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eval_head_preserves_schema() {
        let mut ctx = new_context();
        eval_read(
            &mut ctx,
            vec![Expr::Literal(Literal::String(
                "fixtures/table.parquet".into(),
            ))],
        )
        .await
        .expect("read");

        let original_schema = ctx.batches.as_ref().unwrap()[0].schema();
        let args = parse_fn_args("head(1)");
        eval_head(&mut ctx, args).expect("head");

        let batches = ctx.batches.as_ref().expect("batches");
        assert_eq!(batches[0].schema(), original_schema);
    }

    #[test]
    fn test_eval_head_no_preceding_read() {
        let mut ctx = new_context();
        let args = parse_fn_args("head(5)");
        let result = eval_head(&mut ctx, args);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::GenericError(_)));
    }

    #[test]
    fn test_eval_head_bad_args_string() {
        let mut ctx = new_context();
        let args = vec![Expr::Literal(Literal::String("not_a_number".into()))];
        let result = eval_head(&mut ctx, args);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::UnsupportedFunctionCall(_)
        ));
    }

    #[test]
    fn test_eval_head_no_args() {
        let mut ctx = new_context();
        let result = eval_head(&mut ctx, vec![]);
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
        eval(&mut ctx, expr).await.expect("eval");

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
        eval(&mut ctx, expr).await.expect("eval");

        assert!(output_path.exists());
    }
}
