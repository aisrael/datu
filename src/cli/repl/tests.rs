use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Identifier;
use flt::ast::Literal;
use flt::parser::parse_expr;

use super::ColumnSpec;
use super::GroupByKey;
use super::Repl;
use super::SelectItem;
use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::plan::collect_pipe_stages;
use super::plan::extract_head_n;
use super::plan::extract_path_from_args;
use super::plan::extract_sample_n;
use super::plan::extract_select_items;
use super::plan::extract_tail_n;
use super::plan::is_head_call;
use super::plan::is_statement_complete;
use super::plan::plan_pipeline_with_state;
use super::plan::plan_stage;
use super::plan::validate_repl_pipeline_stages;
use super::stage::ReplPipelineStage;
use crate::Error;
use crate::pipeline::DataFramePipeline;
use crate::pipeline::DisplaySlice;
use crate::pipeline::FilterSpec;
use crate::pipeline::Pipeline;
use crate::pipeline::SelectSpec;

fn parse(input: &str) -> Expr {
    let (remainder, expr) = parse_expr(input).expect("parse");
    assert!(remainder.trim().is_empty(), "unconsumed: {remainder:?}");
    expr
}

fn test_repl() -> Repl {
    Repl::new_for_tests().expect("repl for tests")
}

fn pipe_exprs(input: &str) -> Vec<Expr> {
    let expr = parse(input);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    exprs
}

fn plan_pipeline_result(input: &str) -> (Vec<ReplPipelineStage>, bool) {
    plan_pipeline_with_state(pipe_exprs(input)).unwrap()
}

fn plan_pipeline(input: &str) -> Vec<ReplPipelineStage> {
    plan_pipeline_result(input).0
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
fn test_plan_stage_filter() {
    let expr = parse(r#"filter("a > 1")"#);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::Filter {
            sql: "a > 1".to_string(),
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
                SelectItem::column(ColumnSpec::CaseInsensitive("one".into())),
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into()))
            ]
        }
    );
}

#[test]
fn test_is_statement_complete_select_aggregate_only() {
    let expr = parse("select(sum(:quantity))");
    assert!(is_statement_complete(std::slice::from_ref(&expr)));
    let expr = parse("select(:a, sum(:b))");
    assert!(!is_statement_complete(std::slice::from_ref(&expr)));
}

#[test]
fn test_is_statement_complete_grouped_select_one_line() {
    let exprs = pipe_exprs(r#"read("f.parquet") |> group_by(:id) |> select(:id, sum(:qty))"#);
    assert!(is_statement_complete(&exprs));
}

#[test]
fn test_is_statement_complete_select_avg_only() {
    let exprs = pipe_exprs("select(avg(:quantity))");
    assert!(is_statement_complete(&exprs));
}

#[test]
fn test_is_statement_complete_select_min_only() {
    let expr = parse("select(min(:quantity))");
    assert!(is_statement_complete(std::slice::from_ref(&expr)));
    let exprs = pipe_exprs("select(min(:quantity))");
    assert!(is_statement_complete(&exprs));
}

#[test]
fn test_is_statement_complete_select_count_only() {
    let expr = parse("select(count(:quantity))");
    assert!(is_statement_complete(std::slice::from_ref(&expr)));
    let exprs = pipe_exprs("select(count_distinct(:id))");
    assert!(is_statement_complete(&exprs));
}

#[test]
fn test_is_statement_complete_select_then_group_by() {
    let exprs = pipe_exprs(r#"read("f.parquet") |> select(:id, sum(:qty)) |> group_by(:id)"#);
    assert!(is_statement_complete(&exprs));
}

#[test]
fn test_plan_stage_select_aggregates() {
    let qty = ColumnSpec::CaseInsensitive("quantity".into());
    let cases = [
        ("select(sum(:quantity))", SelectItem::sum(qty.clone())),
        ("select(avg(:quantity))", SelectItem::avg(qty.clone())),
        ("select(min(:quantity))", SelectItem::min(qty.clone())),
        ("select(max(:quantity))", SelectItem::max(qty.clone())),
        ("select(count(:quantity))", SelectItem::count(qty.clone())),
        (
            "select(count_distinct(:quantity))",
            SelectItem::count_distinct(qty),
        ),
    ];
    for (input, expected_col) in cases {
        let expr = parse(input);
        let stage = plan_stage(expr).unwrap();
        assert_eq!(
            stage,
            ReplPipelineStage::Select {
                columns: vec![expected_col],
            },
            "case: {input}"
        );
    }
}

#[test]
fn test_plan_stage_select_with_alias() {
    let expr = parse("select(:foo, foo_bar: :bar, total: sum(:qty))");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("foo".into())),
                SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar"),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())).with_alias("total"),
            ]
        }
    );
}

#[test]
fn test_plan_stage_select_with_quoted_alias_key() {
    let expr = parse(r#"select("foo bar": :bar)"#);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo bar"),
            ]
        }
    );
}

#[test]
fn test_is_statement_complete_select_aliased_aggregate_only() {
    let exprs = pipe_exprs("select(total: sum(:qty))");
    assert!(is_statement_complete(&exprs));
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
    let pipeline = plan_pipeline(r#"read("a.parquet") |> select(:x) |> write("b.csv")"#);
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
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))]
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
fn test_plan_pipeline_auto_appends_print_after_slice() {
    let read_a = ReplPipelineStage::Read {
        path: "a.parquet".to_string(),
    };
    let cases = [
        ("head", "head(5)", ReplPipelineStage::Head { n: 5 }),
        ("tail", "tail(5)", ReplPipelineStage::Tail { n: 5 }),
        ("sample", "sample(5)", ReplPipelineStage::Sample { n: 5 }),
    ];
    for (name, middle, expected_slice) in cases {
        let input = format!(r#"read("a.parquet") |> {middle}"#);
        let pipeline = plan_pipeline(&input);
        assert_eq!(pipeline.len(), 3, "case: {name}");
        assert_eq!(pipeline[0], read_a, "case: {name}");
        assert_eq!(pipeline[1], expected_slice, "case: {name}");
        assert_eq!(pipeline[2], ReplPipelineStage::Print, "case: {name}");
    }
}

#[test]
fn test_plan_pipeline_no_print_when_write_follows_slice() {
    let cases = [
        ("head", "head(5)"),
        ("tail", "tail(5)"),
        ("sample", "sample(5)"),
    ];
    for (name, middle) in cases {
        let input = format!(r#"read("a.parquet") |> {middle} |> write("b.csv")"#);
        let pipeline = plan_pipeline(&input);
        assert_eq!(pipeline.len(), 3, "case: {name}");
        assert!(
            matches!(pipeline.last(), Some(ReplPipelineStage::Write { .. })),
            "case: {name}"
        );
    }
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
    let stages = pipe_exprs(r#"read("a.parquet") |> select(:x) |> write("b.csv")"#);
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

// ── extract_select_items ────────────────────────────────────────

#[test]
fn test_extract_select_items_symbols() {
    let args = vec![
        Expr::Literal(Literal::Symbol("one".into())),
        Expr::Literal(Literal::Symbol("two".into())),
    ];
    let result = extract_select_items(&args).unwrap();
    assert_eq!(
        result,
        vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("one".into())),
            SelectItem::column(ColumnSpec::CaseInsensitive("two".into()))
        ]
    );
}

#[test]
fn test_extract_select_items_strings() {
    let args = vec![
        Expr::Literal(Literal::String("col_a".into())),
        Expr::Literal(Literal::String("col_b".into())),
    ];
    let result = extract_select_items(&args).unwrap();
    assert_eq!(
        result,
        vec![
            SelectItem::column(ColumnSpec::Exact("col_a".into())),
            SelectItem::column(ColumnSpec::Exact("col_b".into()))
        ]
    );
}

#[test]
fn test_extract_select_items_idents() {
    let args = vec![Expr::Ident("foo".into()), Expr::Ident("bar".into())];
    let result = extract_select_items(&args).unwrap();
    assert_eq!(
        result,
        vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("foo".into())),
            SelectItem::column(ColumnSpec::CaseInsensitive("bar".into()))
        ]
    );
}

#[test]
fn test_extract_select_items_mixed() {
    let args = vec![
        Expr::Literal(Literal::Symbol("sym".into())),
        Expr::Literal(Literal::String("str".into())),
        Expr::Ident("ident".into()),
    ];
    let result = extract_select_items(&args).unwrap();
    assert_eq!(
        result,
        vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("sym".into())),
            SelectItem::column(ColumnSpec::Exact("str".into())),
            SelectItem::column(ColumnSpec::CaseInsensitive("ident".into()))
        ]
    );
}

#[test]
fn test_extract_select_items_aggregates() {
    let qty = ColumnSpec::CaseInsensitive("quantity".into());
    let cases = [
        ("sum", SelectItem::sum(qty.clone())),
        ("avg", SelectItem::avg(qty.clone())),
        ("min", SelectItem::min(qty.clone())),
        ("max", SelectItem::max(qty.clone())),
        ("count", SelectItem::count(qty.clone())),
        ("count_distinct", SelectItem::count_distinct(qty)),
    ];
    for (fn_name, expected) in cases {
        let args = vec![Expr::FunctionCall(
            Identifier(fn_name.into()),
            vec![Expr::Literal(Literal::Symbol("quantity".into()))],
        )];
        let result = extract_select_items(&args).unwrap();
        assert_eq!(result, vec![expected], "case: {fn_name}");
    }
}

#[test]
fn test_extract_select_items_empty() {
    let result = extract_select_items(&[]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_extract_select_items_unsupported_expr() {
    let args = vec![Expr::Literal(Literal::Boolean(true))];
    let result = extract_select_items(&args);
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
fn test_validate_rejects_three_filters() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))],
        },
        ReplPipelineStage::Filter {
            sql: "x > 0".into(),
        },
        ReplPipelineStage::Filter {
            sql: "y < 1".into(),
        },
        ReplPipelineStage::Head { n: 1 },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(msg) if msg.contains("at most two filter")));
}

#[test]
fn test_validate_rejects_two_filters_both_after_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))],
        },
        ReplPipelineStage::Filter {
            sql: "x > 0".into(),
        },
        ReplPipelineStage::Filter {
            sql: "x < 10".into(),
        },
        ReplPipelineStage::Head { n: 1 },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(msg) if msg.contains("strictly between")));
}

#[test]
fn test_validate_accepts_two_filters_straddling_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::Filter {
            sql: "one > 0".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive(
                "one".into(),
            ))],
        },
        ReplPipelineStage::Filter {
            sql: "one < 1000".into(),
        },
        ReplPipelineStage::Head { n: 5 },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_accepts_filter_without_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Filter {
            sql: "id > 0".into(),
        },
        ReplPipelineStage::Head { n: 5 },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_accepts_select_filter_head() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive(
                "one".into(),
            ))],
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::Head { n: 5 },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_accepts_filter_after_group_by_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("two".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("three".into())),
            ],
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::Head { n: 3 },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_accepts_filter_group_by_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("two".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("three".into())),
            ],
        },
        ReplPipelineStage::Head { n: 3 },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_builder_bridge_post_aggregate_filter_runs_after_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("two".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("three".into())),
            ],
        },
        ReplPipelineStage::Filter {
            sql: "sum(three) > 0".into(),
        },
        ReplPipelineStage::Head { n: 3 },
    ];
    let mut builder = repl_stages_to_pipeline_builder(&stages).unwrap();
    let Pipeline::DataFrame(p) = builder.build().unwrap() else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(p.filter_before_select, None);
    assert_eq!(
        p.filter_after_select,
        Some(FilterSpec::new("sum(three) > 0"))
    );
}

#[test]
fn test_builder_bridge_where_and_having_filters() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("two".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("three".into())),
            ],
        },
        ReplPipelineStage::Filter {
            sql: "sum(three) > 0".into(),
        },
        ReplPipelineStage::Head { n: 3 },
    ];
    let mut builder = repl_stages_to_pipeline_builder(&stages).unwrap();
    let Pipeline::DataFrame(p) = builder.build().unwrap() else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(p.filter_before_select, Some(FilterSpec::new("true")));
    assert_eq!(
        p.filter_after_select,
        Some(FilterSpec::new("sum(three) > 0"))
    );
}

#[test]
fn test_builder_bridge_sets_filter_sql() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive(
                "one".into(),
            ))],
        },
        ReplPipelineStage::Filter { sql: "true".into() },
        ReplPipelineStage::Head { n: 2 },
    ];
    let mut builder = repl_stages_to_pipeline_builder(&stages).unwrap();
    let Pipeline::DataFrame(p) = builder.build().unwrap() else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(p.filter_before_select, None);
    assert_eq!(p.filter_after_select, Some(FilterSpec::new("true")));
    assert_eq!(p.slice, Some(DisplaySlice::Head(2)));
}

#[test]
fn test_validate_rejects_second_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("y".into()))],
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
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))],
        },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_validate_accepts_read_aggregate_select_only() {
    let q = ColumnSpec::CaseInsensitive("q".into());
    let aggregates = [
        SelectItem::sum(q.clone()),
        SelectItem::avg(q.clone()),
        SelectItem::min(q.clone()),
        SelectItem::max(q.clone()),
        SelectItem::count(q.clone()),
        SelectItem::count_distinct(q),
    ];
    for item in aggregates {
        let stages = vec![
            ReplPipelineStage::Read {
                path: "a.parquet".into(),
            },
            ReplPipelineStage::Select {
                columns: vec![item],
            },
        ];
        validate_repl_pipeline_stages(&stages).unwrap();
    }
}

#[test]
fn test_plan_stage_group_by() {
    let expr = parse("group_by(:id, :region)");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::GroupBy {
            columns: vec![
                GroupByKey::new(ColumnSpec::CaseInsensitive("id".into())),
                GroupByKey::new(ColumnSpec::CaseInsensitive("region".into())),
            ],
        }
    );
}

#[test]
fn test_plan_stage_group_by_with_alias() {
    let expr = parse("group_by(:foo, foo_bar: :bar)");
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::GroupBy {
            columns: vec![
                GroupByKey::new(ColumnSpec::CaseInsensitive("foo".into())),
                GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar"),
            ],
        }
    );
}

#[test]
fn test_plan_stage_group_by_with_quoted_alias_key() {
    let expr = parse(r#"group_by("foo bar": :bar)"#);
    let stage = plan_stage(expr).unwrap();
    assert_eq!(
        stage,
        ReplPipelineStage::GroupBy {
            columns: vec![
                GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo bar"),
            ],
        }
    );
}

#[test]
fn test_validate_grouped_select_matches_aliased_group_by_key_by_underlying_column() {
    // select() may still reference the group key by its underlying column instead of its
    // group_by() alias.
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![
                GroupByKey::new(ColumnSpec::CaseInsensitive("id".into())).with_alias("key"),
            ],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
            ],
        },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_grouped_select_matches_aliased_group_by_key_by_alias() {
    // select() may also reference the group key by the alias assigned in group_by().
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![
                GroupByKey::new(ColumnSpec::CaseInsensitive("id".into())).with_alias("key"),
            ],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("key".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
            ],
        },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_validate_group_by_select_either_order() {
    let gb_then_sel = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
            ],
        },
    ];
    validate_repl_pipeline_stages(&gb_then_sel).unwrap();

    let sel_then_gb = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
            ],
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()))],
        },
    ];
    validate_repl_pipeline_stages(&sel_then_gb).unwrap();
}

#[test]
fn test_validate_rejects_mixed_select_without_group_by() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
                SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
            ],
        },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_validate_rejects_extra_plain_column_with_group_by() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
                SelectItem::column(ColumnSpec::CaseInsensitive("extra".into())),
            ],
        },
    ];
    let err = validate_repl_pipeline_stages(&stages).unwrap_err();
    assert!(matches!(err, Error::InvalidReplPipeline(_)));
}

#[test]
fn test_validate_accepts_column_only_grouped_select() {
    let stages = vec![
        ReplPipelineStage::Read {
            path: "a.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: vec![GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()))],
        },
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("id".into()))],
        },
    ];
    validate_repl_pipeline_stages(&stages).unwrap();
}

#[test]
fn test_builder_bridge_merges_group_by_order_agnostic() {
    let keys = vec![GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()))];
    let items = vec![
        SelectItem::column(ColumnSpec::CaseInsensitive("id".into())),
        SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())),
    ];
    let stages_a = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::GroupBy {
            columns: keys.clone(),
        },
        ReplPipelineStage::Select {
            columns: items.clone(),
        },
    ];
    let stages_b = vec![
        ReplPipelineStage::Read {
            path: "fixtures/table.parquet".into(),
        },
        ReplPipelineStage::Select {
            columns: items.clone(),
        },
        ReplPipelineStage::GroupBy {
            columns: keys.clone(),
        },
    ];
    let mut builder_a = repl_stages_to_pipeline_builder(&stages_a).unwrap();
    let mut builder_b = repl_stages_to_pipeline_builder(&stages_b).unwrap();
    let Pipeline::DataFrame(DataFramePipeline { select: sa, .. }) = builder_a.build().unwrap()
    else {
        panic!("expected DataFrame pipeline");
    };
    let Pipeline::DataFrame(DataFramePipeline { select: sb, .. }) = builder_b.build().unwrap()
    else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(
        sa,
        Some(SelectSpec {
            columns: items,
            group_by: Some(keys),
        })
    );
    assert_eq!(sa, sb);
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
    let (pipeline, incomplete) = plan_pipeline_result(r#"read("a.parquet") |> count()"#);
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
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))]
        }
        .is_terminal()
    );
    assert!(
        ReplPipelineStage::Select {
            columns: vec![SelectItem::column(ColumnSpec::CaseInsensitive("x".into()))]
        }
        .is_non_terminal()
    );
    let col_x = ColumnSpec::CaseInsensitive("x".into());
    for item in [
        SelectItem::sum(col_x.clone()),
        SelectItem::avg(col_x.clone()),
        SelectItem::min(col_x.clone()),
        SelectItem::max(col_x.clone()),
        SelectItem::count(col_x.clone()),
        SelectItem::count_distinct(col_x),
    ] {
        assert!(
            ReplPipelineStage::Select {
                columns: vec![item],
            }
            .is_terminal()
        );
    }
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

#[test]
fn test_display_select_with_bare_alias() {
    let stage = ReplPipelineStage::Select {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("foo".into())),
            SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar"),
            SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into())).with_alias("total"),
        ],
    };
    assert_eq!(
        stage.to_string(),
        "select(:foo, foo_bar: :bar, total: sum(:qty))"
    );
}

#[test]
fn test_display_select_with_quoted_alias() {
    let stage = ReplPipelineStage::Select {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo bar"),
        ],
    };
    assert_eq!(stage.to_string(), r#"select("foo bar": :bar)"#);
}

#[test]
fn test_display_group_by_with_bare_alias() {
    let stage = ReplPipelineStage::GroupBy {
        columns: vec![
            GroupByKey::new(ColumnSpec::CaseInsensitive("foo".into())),
            GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar"),
        ],
    };
    assert_eq!(stage.to_string(), "group_by(:foo, foo_bar: :bar)");
}

#[test]
fn test_display_group_by_with_quoted_alias() {
    let stage = ReplPipelineStage::GroupBy {
        columns: vec![
            GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo bar"),
        ],
    };
    assert_eq!(stage.to_string(), r#"group_by("foo bar": :bar)"#);
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
