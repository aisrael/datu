use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Identifier;
use flt::ast::Literal;
use flt::parser::parse_expr;
use tempfile::NamedTempFile;

use super::ColumnSpec;
use super::pipeline::ReplPipelinePlanner;
use super::plan::collect_pipe_stages;
use super::plan::extract_column_specs;
use super::plan::extract_head_n;
use super::plan::extract_path_from_args;
use super::plan::extract_sample_n;
use super::plan::extract_tail_n;
use super::plan::is_head_call;
use super::plan::plan_pipeline_with_state;
use super::plan::plan_stage;
use super::plan::validate_repl_pipeline_stages;
use super::stage::PipelineStage;
use crate::Error;

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
    let mut ctx = ReplPipelinePlanner::new();
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
    let mut ctx = ReplPipelinePlanner::new();
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
    let mut ctx = ReplPipelinePlanner::new();
    let expr = parse(r#"read("fixtures/table.parquet")"#);
    let _ = ctx.eval(expr).expect("eval");
    assert!(ctx.statement_incomplete);
}

#[test]
fn test_eval_clears_statement_incomplete_for_terminal_final_stage() {
    let mut ctx = ReplPipelinePlanner::new();
    let expr = parse(r#"read("fixtures/table.parquet") |> head(1)"#);
    let _ = ctx.eval(expr).expect("eval");
    assert!(!ctx.statement_incomplete);
}

#[test]
fn test_eval_clears_statement_incomplete_for_write_final_stage() {
    let mut ctx = ReplPipelinePlanner::new();
    let expr = parse(r#"read("fixtures/table.parquet") |> write("out.csv")"#);
    let _ = ctx.eval(expr).expect("eval");
    assert!(!ctx.statement_incomplete);
}

#[test]
fn test_eval_incremental_non_terminal_accumulates_state() {
    let mut ctx = ReplPipelinePlanner::new();
    let expr = parse(r#"read("fixtures/table.parquet")"#);
    let pipeline = ctx.eval_incremental(expr).expect("eval_incremental");
    assert!(pipeline.is_none());
    assert!(ctx.statement_incomplete);
    assert_eq!(ctx.pending_exprs.len(), 1);
}

#[test]
fn test_eval_incremental_terminal_flushes_accumulated_pipeline() {
    let mut ctx = ReplPipelinePlanner::new();
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
    let mut ctx = ReplPipelinePlanner::new();
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

// ── extract_path_from_args ──────────────────────────────────────

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

    let mut ctx = ReplPipelinePlanner::new();
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

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(tempfile.path().exists());
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

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(tempfile.path().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repl_pipeline_select_one_column_write_parquet() {
    let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
    let output_path = tempfile.path().to_str().expect("Failed to get path");

    let pipeline = format!(
        r#"read("fixtures/table.parquet") |> select(:one) |> write("{}")"#,
        &output_path
    );
    let expr = parse(&pipeline);

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(tempfile.path().exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repl_pipeline_select_exact_column_case_errors_at_execute() {
    let tempfile = NamedTempFile::with_suffix(".parquet").expect("Failed to create temp file");
    let output_path = tempfile.path().to_str().expect("Failed to get path");

    let pipeline = format!(
        r#"read("fixtures/table.parquet") |> select("ONE") |> write("{}")"#,
        &output_path
    );
    let expr = parse(&pipeline);

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    let result = ctx.execute_pipeline(pipeline_stages).await;
    assert!(result.is_err());
}

// ── validate_repl_pipeline_stages / plan not implemented ──────

#[test]
fn test_validate_rejects_second_select() {
    let stages = vec![
        PipelineStage::Read {
            path: "a.parquet".into(),
        },
        PipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())],
        },
        PipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("y".into())],
        },
        PipelineStage::Head { n: 1 },
        PipelineStage::Print,
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_validate_rejects_head_before_select() {
    let stages = vec![
        PipelineStage::Read {
            path: "a.parquet".into(),
        },
        PipelineStage::Head { n: 1 },
        PipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())],
        },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_plan_stage_count() {
    let expr = parse("count()");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, PipelineStage::Count);
}

#[test]
fn test_plan_stage_schema() {
    let expr = parse("schema()");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, PipelineStage::Schema);
}

#[test]
fn test_plan_pipeline_read_count() {
    let expr = parse(r#"read("a.parquet") |> count()"#);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    let (pipeline, incomplete) = plan_pipeline_with_state(exprs).unwrap();
    assert!(!incomplete);
    assert_eq!(pipeline.len(), 2);
    assert_eq!(
        pipeline[0],
        PipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(pipeline[1], PipelineStage::Count);
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

fn parse_fn_args(input: &str) -> Vec<Expr> {
    match parse(input) {
        Expr::FunctionCall(_, args) => args,
        other => panic!("expected FunctionCall, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repl_pipeline_read_head_write() {
    let tempfile = NamedTempFile::with_suffix(".csv").expect("Failed to create temp file");
    let output_path = tempfile.path().to_string_lossy().to_string();

    let pipeline = format!(
        r#"read("fixtures/table.parquet") |> head(2) |> write("{}")"#,
        &output_path.replace('\\', "\\\\")
    );
    let expr = parse(&pipeline);

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(tempfile.path().exists());
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

    let mut ctx = ReplPipelinePlanner::new();
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
    assert!(PipelineStage::Schema.is_terminal());
    assert!(PipelineStage::Count.is_terminal());
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
        PipelineStage::Head { n: 5 }.get_implicit_followup_stage(),
        Some(PipelineStage::Print)
    );
    assert_eq!(
        PipelineStage::Tail { n: 5 }.get_implicit_followup_stage(),
        Some(PipelineStage::Print)
    );
    assert_eq!(
        PipelineStage::Write {
            path: "out.csv".into()
        }
        .get_implicit_followup_stage(),
        None
    );
    assert_eq!(PipelineStage::Schema.get_implicit_followup_stage(), None);
    assert_eq!(PipelineStage::Count.get_implicit_followup_stage(), None);
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

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(output_path.exists());
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

    let mut ctx = ReplPipelinePlanner::new();
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
        PipelineStage::Sample { n: 5 }.get_implicit_followup_stage(),
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

    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");

    assert!(output_path.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repl_pipeline_read_schema() {
    let expr = parse(r#"read("fixtures/table.parquet") |> schema()"#);
    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_repl_pipeline_read_count() {
    let expr = parse(r#"read("fixtures/table.parquet") |> count()"#);
    let mut ctx = ReplPipelinePlanner::new();
    let pipeline_stages = ctx.eval(expr).expect("eval");
    ctx.execute_pipeline(pipeline_stages)
        .await
        .expect("execute");
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

    let mut ctx = ReplPipelinePlanner::new();
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
