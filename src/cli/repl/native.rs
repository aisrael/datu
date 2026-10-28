//! datu functions registered as flt natives: flt evaluates the whole statement and calls back
//! into datu for each function.

use std::fmt;

use flt::runtime::Map;
use flt::runtime::MapKey;
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
use crate::pipeline::GroupByKey;
use crate::pipeline::Pipeline;
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

const SELECT_AGG_EXPECTED: &str = "select expects column names, sum(column), avg(column), min(column), max(column), count(column), or count_distinct(column)";

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

/// A `select()` value: a column, or an aggregate such as `sum(:b)` that flt has already
/// evaluated to a [`DatuSelectItem`].
fn select_item_from_value(v: &Value) -> crate::Result<SelectItem> {
    match (v.get_native::<DatuSelectItem>(), v) {
        (Some(item), _) => Ok(item.0.clone()),
        (None, Value::Symbol(s)) => Ok(SelectItem::column(ColumnSpec::CaseInsensitive(s.clone()))),
        (None, Value::String(s)) => Ok(SelectItem::column(ColumnSpec::Exact(s.clone()))),
        (None, other) => Err(Error::UnsupportedFunctionCall(format!(
            "{SELECT_AGG_EXPECTED}, got {other}"
        ))),
    }
}

/// An alias from a keyword argument's name. flt gives every keyword a symbol key, quoted or
/// not (`total:` and `"total amount":`), so a string key means a host built the call.
fn alias_from_key(func: &str, key: &MapKey) -> crate::Result<String> {
    match key {
        MapKey::Symbol(alias) => Ok(alias.clone()),
        MapKey::String(s) => Err(Error::UnsupportedFunctionCall(format!(
            "{func} expects symbol keyword names, got the string {s:?}"
        ))),
    }
}

/// A row count argument such as `head(plan, n)`'s `n`, using flt's conversion so a non-number
/// or a negative number gets flt's own error.
fn usize_arg(v: &Value) -> crate::Result<usize> {
    Ok(usize::try_from(v)?)
}

fn select_aggregate_item(name: &str, col: ColumnSpec) -> SelectItem {
    match name {
        "sum" => SelectItem::sum(col),
        "avg" => SelectItem::avg(col),
        "min" => SelectItem::min(col),
        "max" => SelectItem::max(col),
        "count" => SelectItem::count(col),
        "count_distinct" => SelectItem::count_distinct(col),
        _ => unreachable!(
            "select_aggregate_item only called for sum, avg, min, max, count, or count_distinct"
        ),
    }
}

/// Validates the stages and builds them the way every REPL pipeline is built.
fn build_pipeline(stages: &[ReplPipelineStage]) -> crate::Result<Pipeline> {
    validate_repl_pipeline_stages(stages)?;
    let mut builder = repl_stages_to_pipeline_builder(stages)?;
    apply_repl_display_options(&mut builder);
    builder.build()
}

/// Runs a plan that a statement evaluated to, showing its rows. `head`, `tail` and `sample`
/// get the implicit `print()` the REPL has always added after them.
pub(super) fn display_plan(plan: &DatuPlan) -> crate::Result<()> {
    let mut stages = plan.stages.clone();
    if let Some(print) = stages
        .last()
        .and_then(ReplPipelineStage::get_implicit_followup_stage)
    {
        stages.push(print);
    }
    build_pipeline(&stages)?.execute()
}

/// Runs `plan` with `stage` appended, for the functions that act immediately (`schema`,
/// `write`) rather than return a plan.
fn run_with_stage(plan: &DatuPlan, stage: ReplPipelineStage) -> crate::Result<Value> {
    let mut stages = plan.stages.clone();
    stages.push(stage);
    build_pipeline(&stages)?.execute()?;
    Ok(Value::None)
}

/// Clones `plan` and appends `stage`, for the functions that extend a plan lazily.
fn with_stage(plan: &DatuPlan, stage: ReplPipelineStage) -> Value {
    let mut plan = plan.clone();
    plan.stages.push(stage);
    Value::native(plan)
}

fn read(args: &[Value]) -> crate::Result<Value> {
    let path = string_arg("read", &args[0])?;
    Ok(Value::native(DatuPlan {
        stages: vec![ReplPipelineStage::Read { path }],
    }))
}

fn filter(args: &[Value]) -> crate::Result<Value> {
    let plan = args[0].downcast_native::<DatuPlan>()?;
    let sql = string_arg("filter", &args[1])?;
    Ok(with_stage(plan, ReplPipelineStage::Filter { sql }))
}

const DEFAULT_HEAD_SIZE: usize = 10;

/// `head(plan, n)`, or `head(path, n)` as shorthand for `read(path) |> head(n)`.
fn head(args: &[Value], keywords: &Map) -> crate::Result<Value> {
    // Only the one-argument overloads declare the `n` keyword, and flt fills in its default.
    let n = args
        .get(1)
        .or_else(|| keywords.get(&MapKey::symbol("n")))
        .expect("flt fills in head's n keyword");
    let stage = ReplPipelineStage::Head { n: usize_arg(n)? };
    match args[0].get_native::<DatuPlan>() {
        Some(plan) => Ok(with_stage(plan, stage)),
        None => {
            let path = string_arg("head", &args[0])?;
            Ok(Value::native(DatuPlan {
                stages: vec![ReplPipelineStage::Read { path }, stage],
            }))
        }
    }
}

fn tail(args: &[Value]) -> crate::Result<Value> {
    let plan = args[0].downcast_native::<DatuPlan>()?;
    let n = usize_arg(&args[1])?;
    Ok(with_stage(plan, ReplPipelineStage::Tail { n }))
}

/// Rows `sample(plan)` takes when no count is given.
const DEFAULT_SAMPLE_SIZE: usize = 10;

/// `sample(plan)` or `sample(plan, n)`: flt accepts either overload, so the argument count
/// tells them apart.
fn sample(args: &[Value]) -> crate::Result<Value> {
    let plan = args[0].downcast_native::<DatuPlan>()?;
    let n = match args.get(1) {
        Some(n) => usize_arg(n)?,
        None => DEFAULT_SAMPLE_SIZE,
    };
    Ok(with_stage(plan, ReplPipelineStage::Sample { n }))
}

fn schema(args: &[Value]) -> crate::Result<Value> {
    let plan = args[0].downcast_native::<DatuPlan>()?;
    run_with_stage(plan, ReplPipelineStage::Schema)
}

fn write(args: &[Value]) -> crate::Result<Value> {
    let plan = args[0].downcast_native::<DatuPlan>()?;
    let path = string_arg("write", &args[1])?;
    run_with_stage(plan, ReplPipelineStage::Write { path })
}

/// `select(plan, ...columns, alias: column, ...)`: positional items first, then aliased ones in
/// call order.
fn select(args: &[Value], aliases: &Map) -> crate::Result<Value> {
    let mut plan = args[0].downcast_native::<DatuPlan>()?.clone();
    let mut columns = args[1..]
        .iter()
        .map(select_item_from_value)
        .collect::<crate::Result<Vec<_>>>()?;
    for (key, value) in aliases {
        columns.push(select_item_from_value(value)?.with_alias(alias_from_key("select", key)?));
    }
    // flt's arity check accepts select(plan), so the "at least one" rule lives here.
    if columns.is_empty() {
        return Err(Error::UnsupportedFunctionCall(
            "select expects at least one column name".to_string(),
        ));
    }
    plan.stages.push(ReplPipelineStage::Select { columns });
    Ok(Value::native(plan))
}

/// `group_by(plan, ...keys, alias: key, ...)`, with the same ordering as [`select`].
fn group_by(args: &[Value], aliases: &Map) -> crate::Result<Value> {
    let mut plan = args[0].downcast_native::<DatuPlan>()?.clone();
    let mut columns = args[1..]
        .iter()
        .map(|v| Ok(GroupByKey::new(column_spec_from_value("group_by", v)?)))
        .collect::<crate::Result<Vec<_>>>()?;
    for (key, value) in aliases {
        let spec = column_spec_from_value("group_by", value)?;
        columns.push(GroupByKey::new(spec).with_alias(alias_from_key("group_by", key)?));
    }
    if columns.is_empty() {
        return Err(Error::UnsupportedFunctionCall(
            "group_by expects at least one column".to_string(),
        ));
    }
    plan.stages.push(ReplPipelineStage::GroupBy { columns });
    Ok(Value::native(plan))
}

/// `sum(:col)`, `avg(:col)`, ...: an aggregate for use inside `select(...)`.
fn aggregate(name: &str, args: &[Value]) -> crate::Result<Value> {
    let spec = column_spec_from_value(name, &args[0])?;
    Ok(Value::native(DatuSelectItem(select_aggregate_item(
        name, spec,
    ))))
}

/// Runs the plan with `count()` appended and returns the row count instead of printing it.
fn count_plan_rows(plan: &DatuPlan) -> crate::Result<usize> {
    let mut stages = plan.stages.clone();
    stages.push(ReplPipelineStage::Count);
    build_pipeline(&stages)?.execute_count()
}

/// One handler serves both overloads; flt has already checked the argument against the one it
/// picked, so a non-plan here is a symbol or string.
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
    // flt has no positional defaults, so each one-argument overload takes n as a keyword. The
    // keyword stays off the two-argument overloads so n can't be passed both ways at once.
    runtime.register_function(NativeFunction::with_keywords(
        FunctionDefinition::new(
            "head",
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::number("n"),
            ],
        )
        .add_overload(
            Type::native::<DatuPlan>(),
            vec![Argument::new("plan", Type::native::<DatuPlan>())],
        )
        .keyword("n", Type::number(), DEFAULT_HEAD_SIZE)
        .add_overload(
            Type::native::<DatuPlan>(),
            vec![Argument::string("path"), Argument::number("n")],
        )
        .add_overload(Type::native::<DatuPlan>(), vec![Argument::string("path")])
        .keyword("n", Type::number(), DEFAULT_HEAD_SIZE),
        |args: &[Value], keywords: &Map| to_flt(head(args, keywords)),
    ))?;
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "tail",
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::number("n"),
            ],
        ),
        |args: &[Value]| to_flt(tail(args)),
    ))?;
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "sample",
            Type::native::<DatuPlan>(),
            vec![Argument::new("plan", Type::native::<DatuPlan>())],
        )
        .add_overload(
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::number("n"),
            ],
        ),
        |args: &[Value]| to_flt(sample(args)),
    ))?;
    // schema and write act immediately, so a write inside a larger expression still writes.
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "schema",
            Type::none(),
            vec![Argument::new("plan", Type::native::<DatuPlan>())],
        ),
        |args: &[Value]| to_flt(schema(args)),
    ))?;
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "write",
            Type::none(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::string("path"),
            ],
        ),
        |args: &[Value]| to_flt(write(args)),
    ))?;
    // flt picks the overload by the argument's type: a plan counts rows, a column is an aggregate.
    runtime.register_function(NativeFunction::new(
        FunctionDefinition::new(
            "count",
            Type::number(),
            vec![Argument::new("plan", Type::native::<DatuPlan>())],
        )
        .add_overload(
            Type::native::<DatuSelectItem>(),
            vec![Argument::new(
                "column",
                Type::union(Type::symbol(), Type::string()),
            )],
        ),
        |args: &[Value]| to_flt(count(args)),
    ))?;
    // Columns are variadic and aliases are keywords, so `select(plan)` and
    // `select(plan, total: sum(:b))` both pass flt's checks and the handler validates the rest.
    runtime.register_function(NativeFunction::with_keywords(
        FunctionDefinition::new(
            "select",
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::variadic("columns", Type::any()),
            ],
        )
        .rest_keywords(Type::any()),
        |args: &[Value], aliases: &Map| to_flt(select(args, aliases)),
    ))?;
    runtime.register_function(NativeFunction::with_keywords(
        FunctionDefinition::new(
            "group_by",
            Type::native::<DatuPlan>(),
            vec![
                Argument::new("plan", Type::native::<DatuPlan>()),
                Argument::variadic("keys", Type::any()),
            ],
        )
        .rest_keywords(Type::any()),
        |args: &[Value], aliases: &Map| to_flt(group_by(args, aliases)),
    ))?;
    // count(:col) is the count overload above.
    for name in ["sum", "avg", "min", "max", "count_distinct"] {
        runtime.register_function(NativeFunction::new(
            FunctionDefinition::new(
                name,
                Type::native::<DatuSelectItem>(),
                vec![Argument::new("column", Type::any())],
            ),
            move |args: &[Value]| to_flt(aggregate(name, args)),
        ))?;
    }
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
            "fn(DatuPlan) -> Number | fn(Symbol | String) -> SelectItem"
        );
    }

    #[test]
    fn test_count_rejects_non_columns() {
        assert_eq!(
            eval_err("count(1)"),
            "Runtime Error: No overload of count accepts (Number)"
        );
    }

    #[test]
    fn test_filter_rejects_non_plan() {
        assert_eq!(
            eval_err(r#"filter(1, "a > 1")"#),
            "Runtime Error: Argument plan of filter expects DatuPlan, found Number"
        );
    }

    #[test]
    fn test_read_rejects_non_string() {
        assert_eq!(
            eval_err("read(1)"),
            "Runtime Error: Argument path of read expects String, found Number"
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

    #[test]
    fn test_select_count() {
        assert_eq!(
            eval_to_string(r#"read("fixtures/table.parquet") |> select(:one, "two") |> count()"#),
            "3"
        );
    }

    #[test]
    fn test_group_by_select_count() {
        // userdata.parquet has three gender groups: Female, Male and empty.
        assert_eq!(
            eval_to_string(
                r#"read("fixtures/userdata.parquet") |> group_by(:gender) |> select(:gender, n: count(:id)) |> count()"#
            ),
            "3"
        );
    }

    #[test]
    fn test_select_prints_aliases_in_call_order() {
        assert_eq!(
            eval_to_string(r#"read("x.parquet") |> select(:a, z: :b, "c d": sum(:c))"#),
            r#"read("x.parquet") |> select(:a, z: :b, "c d": sum(:c))"#
        );
        assert_eq!(
            eval_to_string(r#"read("x.parquet") |> group_by(:a, k: :b)"#),
            r#"read("x.parquet") |> group_by(:a, k: :b)"#
        );
    }

    #[test]
    fn test_select_requires_a_column_or_alias() {
        assert_eq!(
            eval_err(r#"read("x.parquet") |> select()"#),
            "Unsupported function call: select expects at least one column name"
        );
        assert_eq!(
            eval_to_string(r#"read("x.parquet") |> select(total: sum(:b))"#),
            r#"read("x.parquet") |> select(total: sum(:b))"#
        );
    }

    #[test]
    fn test_group_by_requires_a_key() {
        assert_eq!(
            eval_err(r#"read("x.parquet") |> group_by()"#),
            "Unsupported function call: group_by expects at least one column"
        );
    }

    #[test]
    fn test_select_rejects_non_columns() {
        let err = eval_err(r#"read("x.parquet") |> select(1)"#);
        assert!(err.contains("select expects column names"), "{err}");
    }

    #[test]
    fn test_group_by_rejects_aggregates() {
        let err = eval_err(r#"read("x.parquet") |> group_by(sum(:a))"#);
        assert!(err.contains("group_by expects a column"), "{err}");
    }

    #[test]
    fn test_aggregates_are_select_items() {
        for name in ["sum", "avg", "min", "max", "count", "count_distinct"] {
            assert_eq!(
                eval_to_string(&format!("{name}(:a)")),
                format!("{name}(:a)")
            );
            assert_eq!(
                eval_to_string(&format!("typeof({name}(:a)) == SelectItem")),
                "true"
            );
        }
    }

    #[test]
    fn test_typeof_select_and_group_by() {
        assert_eq!(
            eval_to_string("typeof(select)"),
            "fn(DatuPlan, *Any, **Any) -> DatuPlan"
        );
        assert_eq!(
            eval_to_string("typeof(group_by)"),
            "fn(DatuPlan, *Any, **Any) -> DatuPlan"
        );
        assert_eq!(eval_to_string("typeof(sum)"), "fn(Any) -> SelectItem");
    }

    #[test]
    fn test_keywords_rejected_where_undeclared() {
        let err = eval_err(r#"read("x.parquet", n: 1)"#);
        assert!(err.contains("keyword"), "{err}");
    }

    #[test]
    fn test_head_tail_sample_extend_the_plan() {
        for (src, printed) in [
            (
                r#"read("x.parquet") |> head(5)"#,
                r#"read("x.parquet") |> head(5)"#,
            ),
            // Without a count, head takes 10 rows; n can also be passed as a keyword.
            (
                r#"read("x.parquet") |> head()"#,
                r#"read("x.parquet") |> head(10)"#,
            ),
            (
                r#"read("x.parquet") |> head(n: 5)"#,
                r#"read("x.parquet") |> head(5)"#,
            ),
            // head(path, ...) is shorthand for read(path) |> head(...).
            (r#"head("x.parquet")"#, r#"read("x.parquet") |> head(10)"#),
            (r#"head("x.parquet", 5)"#, r#"read("x.parquet") |> head(5)"#),
            (
                r#"head("x.parquet", n: 50)"#,
                r#"read("x.parquet") |> head(50)"#,
            ),
            (
                r#"read("x.parquet") |> tail(2)"#,
                r#"read("x.parquet") |> tail(2)"#,
            ),
            (
                r#"read("x.parquet") |> sample(3)"#,
                r#"read("x.parquet") |> sample(3)"#,
            ),
            // Without a count, sample takes 10 rows, as it always has.
            (
                r#"read("x.parquet") |> sample()"#,
                r#"read("x.parquet") |> sample(10)"#,
            ),
        ] {
            assert_eq!(eval_to_string(src), printed, "{src}");
        }
    }

    #[test]
    fn test_head_rejects_bad_counts() {
        assert_eq!(
            eval_err(r#"read("x.parquet") |> head("x")"#),
            "Runtime Error: No overload of head accepts (DatuPlan, String)"
        );
        assert_eq!(
            eval_err(r#"read("x.parquet") |> head(n: "x")"#),
            "Runtime Error: No overload of head accepts (DatuPlan, n: String)"
        );
        assert_eq!(
            eval_err(r#"read("x.parquet") |> head(-1)"#),
            "Runtime Error: Cannot convert -1 to usize"
        );
    }

    #[test]
    fn test_head_rejects_n_passed_both_ways() {
        assert_eq!(
            eval_err(r#"read("x.parquet") |> head(1, n: 2)"#),
            "Runtime Error: No overload of head accepts (DatuPlan, Number, n: Number)"
        );
        assert_eq!(
            eval_err(r#"head("x.parquet", 1, n: 2)"#),
            "Runtime Error: No overload of head accepts (String, Number, n: Number)"
        );
    }

    #[test]
    fn test_head_rejects_unknown_keywords() {
        let err = eval_err(r#"head("x.parquet", m: 2)"#);
        assert!(err.contains("m"), "{err}");
    }

    #[test]
    fn test_select_rejects_a_map_of_aliases() {
        // Aliases are keyword arguments; a map literal is just a value that isn't a column.
        let err = eval_err(r#"read("x.parquet") |> select({total: sum(:b)})"#);
        assert!(err.contains("select expects column names"), "{err}");
    }

    #[test]
    fn test_schema_runs_and_returns_none() {
        assert_eq!(
            eval_to_string(r#"read("fixtures/table.parquet") |> schema()"#),
            "None"
        );
    }

    #[test]
    fn test_write_runs_and_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.csv");
        let src = format!(
            r#"read("fixtures/table.parquet") |> head(2) |> write("{}")"#,
            path.display()
        );
        assert_eq!(eval_to_string(&src), "None");
        let written = std::fs::read_to_string(&path).expect("written file");
        // A header and two rows.
        assert_eq!(written.lines().count(), 3, "{written}");
    }

    #[test]
    fn test_write_rejects_invalid_pipelines() {
        let err = eval_err(r#"read("fixtures/table.parquet") |> count() |> write("x.csv")"#);
        assert_eq!(
            err,
            "Runtime Error: Argument plan of write expects DatuPlan, found Number"
        );
    }

    #[test]
    fn test_typeof_terminal_natives() {
        assert_eq!(
            eval_to_string("typeof(head)"),
            "fn(DatuPlan, Number) -> DatuPlan | fn(DatuPlan, n: Number) -> DatuPlan | fn(String, \
             Number) -> DatuPlan | fn(String, n: Number) -> DatuPlan"
        );
        assert_eq!(
            eval_to_string("typeof(sample)"),
            "fn(DatuPlan) -> DatuPlan | fn(DatuPlan, Number) -> DatuPlan"
        );
        assert_eq!(eval_to_string("typeof(schema)"), "fn(DatuPlan) -> None");
        assert_eq!(
            eval_to_string("typeof(write)"),
            "fn(DatuPlan, String) -> None"
        );
    }
}
