use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Identifier;
use flt::ast::Literal;
use flt::parser::parse_expr;

use super::ColumnSpec;
use super::Repl;
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
use super::stage::ReplPipelineStage;
use crate::Error;

fn parse(input: &str) -> Expr {
    let (remainder, expr) = parse_expr(input).expect("parse");
    assert!(remainder.trim().is_empty(), "unconsumed: {remainder:?}");
    expr
}

fn test_repl() -> Repl {
    Repl::new_for_tests().expect("repl for tests")
}

// ── plan_stage ─────────────────────────────────────────────────

#[test]
fn test_plan_stage_read() {
    let expr = parse(r#"read("file.parquet")"#);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::Read {
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
        ReplPipelineStage::Select {
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
    assert_eq!(stage, ReplPipelineStage::Head { n: 5 });
}

#[test]
fn test_plan_stage_write() {
    let expr = parse(r#"write("output.csv")"#);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::Write {
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
        ReplPipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(
        pipeline[1],
        ReplPipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())]
        }
    );
    assert_eq!(
        pipeline[2],
        ReplPipelineStage::Write {
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
        ReplPipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(pipeline[1], ReplPipelineStage::Head { n: 5 });
    assert_eq!(pipeline[2], ReplPipelineStage::Print);
}

#[test]
fn test_plan_pipeline_no_print_when_write_follows_head() {
    let expr = parse(r#"read("a.parquet") |> head(5) |> write("b.csv")"#);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
    assert_eq!(pipeline.len(), 3);
    assert!(matches!(
        pipeline.last(),
        Some(ReplPipelineStage::Write { .. })
    ));
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

#[test]
fn test_eval_incremental_non_terminal_accumulates_state() {
    let mut ctx = test_repl();
    let expr = parse(r#"read("fixtures/table.parquet")"#);
    let pipeline = ctx.eval_incremental(expr).expect("eval_incremental");
    assert!(pipeline.is_none());
    assert!(ctx.statement_incomplete);
    assert_eq!(ctx.pending_exprs.len(), 1);
}

#[test]
fn test_eval_incremental_terminal_flushes_accumulated_pipeline() {
    let mut ctx = test_repl();
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
    assert!(matches!(second_pipeline[0], ReplPipelineStage::Read { .. }));
    assert_eq!(second_pipeline[1], ReplPipelineStage::Head { n: 2 });
    assert_eq!(second_pipeline[2], ReplPipelineStage::Print);
    assert!(!ctx.statement_incomplete);
    assert!(ctx.pending_exprs.is_empty());
}

#[test]
fn test_eval_incremental_terminal_single_input_executes_immediately() {
    let mut ctx = test_repl();
    let expr = parse(r#"read("fixtures/table.parquet") |> write("out.csv")"#);
    let pipeline = ctx
        .eval_incremental(expr)
        .expect("eval_incremental")
        .expect("pipeline should be complete");
    assert_eq!(pipeline.len(), 2);
    assert!(matches!(pipeline[0], ReplPipelineStage::Read { .. }));
    assert!(matches!(pipeline[1], ReplPipelineStage::Write { .. }));
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

// ── validate_repl_pipeline_stages / plan not implemented ──────

#[test]
fn test_validate_rejects_second_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())],
        },
        ReplPipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("y".into())],
        },
        ReplPipelineStage::Head { n: 1 },
        ReplPipelineStage::Print,
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_validate_rejects_head_before_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Head { n: 1 },
        ReplPipelineStage::Select {
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
    assert_eq!(stage, ReplPipelineStage::Count);
}

#[test]
fn test_plan_stage_schema() {
    let expr = parse("schema()");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, ReplPipelineStage::Schema);
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
        ReplPipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(pipeline[1], ReplPipelineStage::Count);
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

// ── plan_stage: tail ─────────────────────────────────────────

#[test]
fn test_plan_stage_tail() {
    let expr = parse("tail(5)");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, ReplPipelineStage::Tail { n: 5 });
}

#[test]
fn test_terminal_stage_classification() {
    assert!(ReplPipelineStage::Head { n: 1 }.is_terminal());
    assert!(ReplPipelineStage::Tail { n: 1 }.is_terminal());
    assert!(ReplPipelineStage::Schema.is_terminal());
    assert!(ReplPipelineStage::Count.is_terminal());
    assert!(
        ReplPipelineStage::Write {
            path: "out.csv".into()
        }
        .is_terminal()
    );
    assert!(
        !ReplPipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())]
        }
        .is_terminal()
    );
    assert!(
        ReplPipelineStage::Select {
            columns: vec![ColumnSpec::CaseInsensitive("x".into())]
        }
        .is_non_terminal()
    );
}

#[test]
fn test_terminal_stage_implicit_followup() {
    assert_eq!(
        ReplPipelineStage::Head { n: 5 }.get_implicit_followup_stage(),
        Some(ReplPipelineStage::Print)
    );
    assert_eq!(
        ReplPipelineStage::Tail { n: 5 }.get_implicit_followup_stage(),
        Some(ReplPipelineStage::Print)
    );
    assert_eq!(
        ReplPipelineStage::Write {
            path: "out.csv".into()
        }
        .get_implicit_followup_stage(),
        None
    );
    assert_eq!(
        ReplPipelineStage::Schema.get_implicit_followup_stage(),
        None
    );
    assert_eq!(ReplPipelineStage::Count.get_implicit_followup_stage(), None);
}

#[test]
fn test_display_print_stage() {
    assert_eq!(ReplPipelineStage::Print.to_string(), "print()");
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
        ReplPipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(pipeline[1], ReplPipelineStage::Tail { n: 5 });
    assert_eq!(pipeline[2], ReplPipelineStage::Print);
}

#[test]
fn test_plan_pipeline_no_print_when_write_follows_tail() {
    let expr = parse(r#"read("a.parquet") |> tail(5) |> write("b.csv")"#);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
    assert_eq!(pipeline.len(), 3);
    assert!(matches!(
        pipeline.last(),
        Some(ReplPipelineStage::Write { .. })
    ));
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

// ── plan_stage: sample ──────────────────────────────────────

#[test]
fn test_plan_stage_sample_with_arg() {
    let expr = parse("sample(5)");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, ReplPipelineStage::Sample { n: 5 });
}

#[test]
fn test_plan_stage_sample_no_arg_defaults_to_10() {
    let expr = Expr::FunctionCall(Identifier("sample".into()), vec![]);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(stage, ReplPipelineStage::Sample { n: 10 });
}

#[test]
fn test_sample_is_terminal() {
    assert!(ReplPipelineStage::Sample { n: 5 }.is_terminal());
}

#[test]
fn test_sample_implicit_followup_is_print() {
    assert_eq!(
        ReplPipelineStage::Sample { n: 5 }.get_implicit_followup_stage(),
        Some(ReplPipelineStage::Print)
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
        ReplPipelineStage::Read {
            path: "a.parquet".to_string()
        }
    );
    assert_eq!(pipeline[1], ReplPipelineStage::Sample { n: 5 });
    assert_eq!(pipeline[2], ReplPipelineStage::Print);
}

#[test]
fn test_plan_pipeline_no_print_when_write_follows_sample() {
    let expr = parse(r#"read("a.parquet") |> sample(5) |> write("b.csv")"#);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    let (pipeline, _) = plan_pipeline_with_state(exprs).unwrap();
    assert_eq!(pipeline.len(), 3);
    assert!(matches!(
        pipeline.last(),
        Some(ReplPipelineStage::Write { .. })
    ));
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

#[test]
fn test_display_sample_stage() {
    assert_eq!(
        ReplPipelineStage::Sample { n: 10 }.to_string(),
        "sample(10)"
    );
}
