//! datu functions registered as flt natives: flt evaluates the whole statement and calls back
//! into datu for each function.

use std::fmt;

use flt::runtime::Runtime;
use flt::runtime::Value;
use flt::runtime::functions::Argument;
use flt::runtime::functions::FunctionDefinition;
use flt::runtime::functions::NativeFunction;
use flt::runtime::native::NativeType;
use flt::runtime::types::Type;

use super::builder_bridge::repl_stages_to_pipeline_builder;
use super::interactive::apply_repl_display_options;
use super::plan::validate_repl_pipeline_stages;
use super::stage::ReplPipelineStage;
use super::stage::format_select_item;
use crate::Error;
use crate::pipeline::ColumnSpec;
use crate::pipeline::SelectItem;

/// A lazy pipeline: the stages built so far by `read(..) |> filter(..) |> ...`. Keeping stages
/// rather than a DataFrame lets the existing validation and builder run unchanged at `count()`.
#[derive(Clone, PartialEq)]
pub(super) struct DatuPlan {
    pub stages: Vec<ReplPipelineStage>,
}

impl fmt::Display for DatuPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stages: Vec<String> = self.stages.iter().map(ToString::to_string).collect();
        write!(f, "{}", stages.join(" |> "))
    }
}

impl NativeType for DatuPlan {
    const NAME: &'static str = "DatuPlan";
}

/// What an aggregate such as `count(:col)` evaluates to, for use inside `select(...)`.
#[derive(Clone, PartialEq)]
pub(super) struct DatuSelectItem(pub SelectItem);

impl fmt::Display for DatuSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_select_item(&self.0))
    }
}

impl NativeType for DatuSelectItem {
    const NAME: &'static str = "SelectItem";
}

/// Handlers do their work as `crate::Result` and convert only here, so datu's own error
/// messages reach the user unchanged.
fn to_flt(result: crate::Result<Value>) -> Result<Value, flt::Error> {
    result.map_err(flt::Error::native)
}

fn string_arg(func: &str, v: &Value) -> crate::Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        other => Err(Error::UnsupportedFunctionCall(format!(
            "{func} expects a string, got {other}"
        ))),
    }
}

fn column_spec_from_value(func: &str, v: &Value) -> crate::Result<ColumnSpec> {
    match v {
        Value::Symbol(s) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
        Value::String(s) => Ok(ColumnSpec::Exact(s.clone())),
        other => Err(Error::UnsupportedFunctionCall(format!(
            "{func} expects a column (symbol or string), got {other}"
        ))),
    }
}

fn read(args: &[Value]) -> crate::Result<Value> {
    let path = string_arg("read", &args[0])?;
    Ok(Value::native(DatuPlan {
        stages: vec![ReplPipelineStage::Read { path }],
    }))
}

fn filter(args: &[Value]) -> crate::Result<Value> {
    let mut plan = args[0].downcast_native::<DatuPlan>()?.clone();
    let sql = string_arg("filter", &args[1])?;
    plan.stages.push(ReplPipelineStage::Filter { sql });
    Ok(Value::native(plan))
}

/// Builds and runs the plan the same way the REPL runs any other pipeline, but returns the row
/// count instead of printing it.
fn count_plan_rows(plan: &DatuPlan) -> crate::Result<usize> {
    let mut stages = plan.stages.clone();
    stages.push(ReplPipelineStage::Count);
    validate_repl_pipeline_stages(&stages)?;
    let mut builder = repl_stages_to_pipeline_builder(&stages)?;
    apply_repl_display_options(&mut builder);
    builder.build()?.execute_count()
}

/// flt checks only arity, so both `count(plan)` and `count(:col)` arrive here.
fn count(args: &[Value]) -> crate::Result<Value> {
    match args[0].get_native::<DatuPlan>() {
        // Terminal stage: read(..) |> ... |> count()
        Some(plan) => Ok(Value::from(count_plan_rows(plan)?)),
        // Aggregate inside select(...): count(:col)
        None => Ok(Value::native(DatuSelectItem(SelectItem::count(
            column_spec_from_value("count", &args[0])?,
        )))),
    }
}

/// Registers datu's native types, then the functions that use them. Called once, before any
/// flt source is evaluated.
pub(super) fn register_natives(runtime: &mut impl Runtime) -> Result<(), flt::Error> {
    // Types first, by flt convention; flt doesn't enforce it.
    runtime.register_native_type::<DatuPlan>()?;
    runtime.register_native_type::<DatuSelectItem>()?;

    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "read",
            Type::native::<DatuPlan>(),
            vec![Argument::string("path")],
        ),
        |args: &[Value]| to_flt(read(args)),
    ))?;
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "filter",
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::string("sql"),
            ],
        ),
        |args: &[Value]| to_flt(filter(args)),
    ))?;
    // The overload exists only so typeof(count) describes both forms; one handler serves both.
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "count",
            Type::number(),
            vec![Argument::new("plan", Type::native::<DatuPlan>())],
        )
        .add_overload(
            Type::native::<DatuSelectItem>(),
            vec![Argument::new("column", Type::any())],
        ),
        |args: &[Value]| to_flt(count(args)),
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use flt::ast::Statement;
    use flt::parser::parse_expr;
    use flt::runtime::SimpleRuntime;

    use super::*;

    fn eval(src: &str) -> Result<Value, flt::Error> {
        let mut runtime = SimpleRuntime::default();
        register_natives(&mut runtime).expect("datu's natives should register");
        let (rest, expr) = parse_expr(src).expect("parse");
        assert!(rest.trim().is_empty(), "unparsed input: {rest:?}");
        runtime.eval_statement(&Statement::Expr(expr))
    }

    fn eval_to_string(src: &str) -> String {
        eval(src).expect("eval").to_string()
    }

    fn eval_err(src: &str) -> String {
        match eval(src) {
            Ok(v) => panic!("expected an error from {src}, got {v}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn test_read_count_parquet() {
        assert_eq!(
            eval_to_string(r#"read("fixtures/table.parquet") |> count()"#),
            "3"
        );
    }

    #[test]
    fn test_read_count_orc() {
        assert_eq!(
            eval_to_string(r#"read("fixtures/userdata.orc") |> count()"#),
            "1000"
        );
    }

    #[test]
    fn test_read_filter_count() {
        assert_eq!(
            eval_to_string(
                r#"read("fixtures/userdata.parquet") |> filter("gender = 'Female'") |> count()"#
            ),
            "482"
        );
    }

    #[test]
    fn test_count_without_read_is_an_arity_error() {
        let err = eval_err("count()");
        assert!(err.contains("expected 1 argument"), "{err}");
    }

    #[test]
    fn test_count_column_is_select_item() {
        assert_eq!(eval_to_string("count(:id)"), "count(:id)");
        assert_eq!(eval_to_string("typeof(count(:id)) == SelectItem"), "true");
    }

    #[test]
    fn test_plan_prints_as_its_pipeline() {
        assert_eq!(
            eval_to_string(r#"read("x.parquet") |> filter("a > 1")"#),
            r#"read("x.parquet") |> filter("a > 1")"#
        );
    }

    #[test]
    fn test_native_types_are_registered() {
        // flt doesn't notice an unregistered native type; its name just fails to resolve.
        assert_eq!(
            eval_to_string(r#"typeof(read("x.parquet")) == DatuPlan"#),
            "true"
        );
    }

    #[test]
    fn test_typeof_natives() {
        assert_eq!(
            eval_to_string("typeof(filter)"),
            "fn(DatuPlan, String) -> DatuPlan"
        );
        assert_eq!(
            eval_to_string("typeof(count)"),
            "fn(DatuPlan) -> Number | fn(Any) -> SelectItem"
        );
    }

    #[test]
    fn test_filter_rejects_non_plan() {
        assert_eq!(
            eval_err(r#"filter(1, "a > 1")"#),
            "Runtime Error: Expected DatuPlan, found Number"
        );
    }

    #[test]
    fn test_read_rejects_non_string() {
        assert_eq!(
            eval_err("read(1)"),
            "Unsupported function call: read expects a string, got 1"
        );
    }

    #[test]
    fn test_wrong_argument_count() {
        let err = eval_err(r#"read("x.parquet") |> count(:a, :b)"#);
        assert!(err.contains("expected 1 argument"), "{err}");
    }

    #[test]
    fn test_datafusion_error_passes_through() {
        let err =
            eval_err(r#"read("fixtures/table.parquet") |> filter("no_such_col > 1") |> count()"#);
        assert!(err.contains("no_such_col"), "{err}");
    }
}
