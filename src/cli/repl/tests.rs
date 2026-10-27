use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;
use flt::ast::Statement;
use flt::parser::parse_expr;
use flt::runtime::Runtime;
use flt::runtime::SimpleRuntime;

use super::ColumnSpec;
use super::DatuRepl;
use super::GroupByKey;
use super::SelectItem;
use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::native::DatuPlan;
use super::native::register_natives;
use super::plan::collect_pipe_stages;
use super::plan::fold_pipe_stages;
use super::plan::is_statement_complete;
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

fn test_repl() -> DatuRepl {
    DatuRepl::default()
}

fn pipe_exprs(input: &str) -> Vec<Expr> {
    let expr = parse(input);
    let mut exprs = Vec::new();
    collect_pipe_stages(expr, &mut exprs);
    exprs
}

/// The stages a pipeline builds, without running it. `count()`, `schema()` and `write(path)`
/// run as soon as flt calls them, so they're split off and appended as stages instead.
fn plan_stages(input: &str) -> Vec<ReplPipelineStage> {
    let mut exprs = pipe_exprs(input);
    let terminal = match exprs.last() {
        Some(Expr::FunctionCall(call)) => {
            match (call.name.0.as_str(), call.positional_args.as_slice()) {
                ("count", []) => Some(ReplPipelineStage::Count),
                ("schema", []) => Some(ReplPipelineStage::Schema),
                ("write", [Expr::Literal(Literal::String(path))]) => {
                    Some(ReplPipelineStage::Write { path: path.clone() })
                }
                _ => None,
            }
        }
        _ => None,
    };
    if terminal.is_some() {
        exprs.pop();
    }
    let mut runtime = SimpleRuntime::default();
    register_natives(&mut runtime).expect("datu's natives should register");
    let expr = fold_pipe_stages(exprs).expect("a pipeline before the terminal stage");
    let value = runtime
        .eval_statement(&Statement::Expr(expr))
        .expect("eval");
    let mut stages = value
        .downcast_native::<DatuPlan>()
        .expect("a plan")
        .stages
        .clone();
    stages.extend(terminal);
    stages
}

// ── is_statement_complete ───────────────────────────────────────

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
fn test_is_statement_complete_select_aliased_aggregate_only() {
    let exprs = pipe_exprs("select(total: sum(:qty))");
    assert!(is_statement_complete(&exprs));
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

// ── validate_repl_pipeline_stages / builder bridge ─────────────

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

// ── stage classification and display ────────────────────────

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
fn test_display_alias_quotes_keys_flt_cannot_parse_bare() {
    // flt's bare keys must start with a letter, so these need quotes to parse back.
    for alias in ["_tmp", "1x"] {
        let stage = ReplPipelineStage::Select {
            columns: vec![
                SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias(alias),
            ],
        };
        let printed = stage.to_string();
        assert_eq!(printed, format!(r#"select("{alias}": :bar)"#));
        let stages = plan_stages(&format!(r#"read("x.parquet") |> {printed}"#));
        let Some(ReplPipelineStage::Select { columns }) = stages.last() else {
            panic!("expected select");
        };
        assert_eq!(columns[0].alias(), Some(alias));
    }
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

#[test]
fn test_display_sample_stage() {
    assert_eq!(
        ReplPipelineStage::Sample { n: 10 }.to_string(),
        "sample(10)"
    );
}

// ── line accumulation ──────────────────────────────────────────

#[test]
fn test_fold_pipe_stages_round_trips_collect() {
    let input = r#"read("a.parquet") |> filter("x > 1") |> count()"#;
    let folded = fold_pipe_stages(pipe_exprs(input)).expect("non-empty");
    assert_eq!(folded.to_string(), input);
    assert!(fold_pipe_stages(Vec::new()).is_none());
}

#[test]
fn test_eval_incremental_returns_complete_statement() {
    let mut ctx = test_repl();
    let statement = ctx
        .eval_incremental(parse(r#"read("fixtures/table.parquet") |> head(1)"#))
        .expect("a complete statement");
    assert_eq!(
        statement.to_string(),
        r#"read("fixtures/table.parquet") |> head(1)"#
    );
    assert!(!ctx.statement_incomplete);
    assert!(ctx.pending_exprs.is_empty());
}

#[test]
fn test_eval_incremental_holds_incomplete_statement() {
    let mut ctx = test_repl();
    assert!(
        ctx.eval_incremental(parse(r#"read("fixtures/table.parquet")"#))
            .is_none()
    );
    assert!(ctx.statement_incomplete);
    assert_eq!(ctx.pending_exprs.len(), 1);
}

#[test]
fn test_execute_shows_each_kind_of_value() {
    let mut ctx = test_repl();
    for input in [
        // A plan: its rows are displayed.
        r#"read("fixtures/table.parquet") |> head(1)"#,
        // A number.
        r#"read("fixtures/table.parquet") |> count()"#,
        // None: schema has already printed.
        r#"read("fixtures/table.parquet") |> schema()"#,
    ] {
        ctx.execute(parse(input)).expect(input);
    }
}

// ── Pipeline::execute_count ────────────────────────────────────

fn execute_count(input: &str) -> usize {
    let stages = plan_stages(input);
    let mut builder = repl_stages_to_pipeline_builder(&stages).expect("builder");
    builder
        .build()
        .expect("build")
        .execute_count()
        .expect("execute_count")
}

#[test]
fn test_execute_count_dataframe() {
    assert_eq!(
        execute_count(r#"read("fixtures/table.parquet") |> count()"#),
        3
    );
    assert_eq!(
        execute_count(
            r#"read("fixtures/userdata.parquet") |> filter("gender = 'Female'") |> count()"#
        ),
        482
    );
    assert_eq!(
        execute_count(r#"read("fixtures/table.parquet") |> select(:one) |> count()"#),
        3
    );
}

#[test]
fn test_execute_count_orc() {
    assert_eq!(
        execute_count(r#"read("fixtures/userdata.orc") |> count()"#),
        1000
    );
    assert_eq!(
        execute_count(r#"read("fixtures/userdata.orc") |> select("_col1") |> count()"#),
        1000
    );
}

#[test]
fn test_execute_count_rejects_non_count_sink() {
    let stages = plan_stages(r#"read("fixtures/table.parquet") |> head(1)"#);
    let mut builder = repl_stages_to_pipeline_builder(&stages).expect("builder");
    assert!(builder.build().expect("build").execute_count().is_err());
}
