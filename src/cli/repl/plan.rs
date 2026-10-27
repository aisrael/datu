//! Parse flt [`Expr`] trees into [`PipelineStage`] lists and validate order.

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::FunctionCall;
use flt::ast::Literal;

use super::stage::ReplPipelineStage;
use super::stage::repl_pipeline_last_select_is_terminal;
use crate::Error;
use crate::pipeline::ColumnSpec;
use crate::pipeline::GroupByKey;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;

/// Collects pipeline stages from a pipe expression (e.g. a |> b |> c -> [a, b, c]).
pub(super) fn collect_pipe_stages(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(l, BinaryOp::Pipe, r) => {
            collect_pipe_stages(*l, out);
            collect_pipe_stages(*r, out);
        }
        other => out.push(other),
    }
}

/// The inverse of [`collect_pipe_stages`]: rebuilds `[a, b, c]` as `(a |> b) |> c`. The REPL
/// pipes across lines implicitly, so the statement has to be rebuilt from parsed exprs; the
/// typed source lines alone aren't valid flt.
pub(super) fn fold_pipe_stages(exprs: Vec<Expr>) -> Option<Expr> {
    exprs
        .into_iter()
        .reduce(|l, r| Expr::BinaryExpr(Box::new(l), BinaryOp::Pipe, Box::new(r)))
}

/// Functions registered as flt natives that may come before `count()`.
const NATIVE_STAGES: &[&str] = &["read", "filter", "group_by", "select"];

/// The called function's name, if `expr` is a function call.
fn call_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::FunctionCall(call) => Some(call.name.0.as_str()),
        _ => None,
    }
}

/// True when a complete statement should be evaluated by flt: it ends in `count()` with no
/// arguments and every earlier stage is a native function call. A bare `count()` stays on the
/// Expr planner, whose "must start with read" error is clearer than flt's arity error.
pub(super) fn is_native_count_statement(pending_exprs: &[Expr]) -> bool {
    let Some((last, body)) = pending_exprs.split_last() else {
        return false;
    };
    let ends_in_count = matches!(
        last,
        Expr::FunctionCall(call)
            if call.name == "count" && call.positional_args.is_empty() && call.keyword_args.is_empty()
    );
    let is_native_stage = |e: &Expr| call_name(e).is_some_and(|name| NATIVE_STAGES.contains(&name));
    ends_in_count && !body.is_empty() && body.iter().all(is_native_stage)
}

fn expr_is_group_by_call(expr: &Expr) -> bool {
    call_name(expr) == Some("group_by")
}

fn expr_is_select_call(expr: &Expr) -> bool {
    call_name(expr) == Some("select")
}

/// True when the accumulated pipe stages form a complete REPL statement (flush and execute).
pub(super) fn is_statement_complete(pending_exprs: &[Expr]) -> bool {
    let Some(Expr::FunctionCall(last)) = pending_exprs.last() else {
        return false;
    };
    let before_last = &pending_exprs[..pending_exprs.len() - 1];
    match last.name.0.as_str() {
        "count" | "head" | "tail" | "sample" | "schema" | "write" => true,
        "group_by" => before_last.iter().any(expr_is_select_call),
        "select" => {
            select_args_are_all_aggregates(last) || before_last.iter().any(expr_is_group_by_call)
        }
        _ => false,
    }
}

fn expr_is_aggregate_call(e: &Expr) -> bool {
    matches!(
        e,
        Expr::FunctionCall(call)
            if matches!(
                call.name.0.as_str(),
                "sum" | "avg" | "min" | "max" | "count" | "count_distinct"
            ) && call.positional_args.len() == 1
                && call.keyword_args.is_empty()
    )
}

/// A call's argument values, positional then keyword (`alias: value`), in source order.
fn call_value_exprs(call: &FunctionCall) -> impl Iterator<Item = &Expr> {
    call.positional_args
        .iter()
        .chain(call.keyword_args.values())
}

fn select_args_are_all_aggregates(call: &FunctionCall) -> bool {
    let mut values = call_value_exprs(call).peekable();
    values.peek().is_some() && values.all(expr_is_aggregate_call)
}

/// Rejects keyword arguments on a function that doesn't take any. They'd otherwise be ignored,
/// because the planner reads only the positional arguments.
fn positional_args_only(call: &FunctionCall) -> crate::Result<&[Expr]> {
    if call.keyword_args.is_empty() {
        Ok(&call.positional_args)
    } else {
        Err(Error::UnsupportedFunctionCall(format!(
            "{} doesn't take keyword arguments, got {call}",
            call.name
        )))
    }
}

/// Extracts a single path string from a function's argument list.
pub(super) fn extract_path_from_args(func_name: &str, args: &[Expr]) -> crate::Result<String> {
    match args {
        [Expr::Literal(Literal::String(s))] => Ok(s.clone()),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "{func_name} expects a single string argument, got {args:?}"
        ))),
    }
}

/// Extracts a single SQL predicate string (same shape as [`extract_path_from_args`]).
pub(super) fn extract_sql_predicate_from_args(
    func_name: &str,
    args: &[Expr],
) -> crate::Result<String> {
    extract_path_from_args(func_name, args)
}

fn extract_one_column_spec(expr: &Expr) -> crate::Result<ColumnSpec> {
    match expr {
        Expr::Literal(Literal::Symbol(s)) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
        Expr::Literal(Literal::String(s)) => Ok(ColumnSpec::Exact(s.clone())),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "expected a column (symbol or string), got {expr:?}"
        ))),
    }
}

pub(super) fn select_aggregate_item(name: &str, col: ColumnSpec) -> SelectItem {
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

pub(super) const SELECT_AGG_EXPECTED: &str = "select expects column names, sum(column), avg(column), min(column), max(column), count(column), or count_distinct(column)";

/// Converts one `select()` value expression (a plain column ref or an aggregate call)
/// into an unaliased [`SelectItem`]. Callers attach an alias separately when the value
/// came from a `name: value` keyword argument.
fn select_item_from_expr(expr: &Expr) -> crate::Result<SelectItem> {
    match expr {
        Expr::FunctionCall(call) => match call.name.0.as_str() {
            name @ ("sum" | "avg" | "min" | "max" | "count" | "count_distinct") => {
                match positional_args_only(call)? {
                    [one] => Ok(select_aggregate_item(name, extract_one_column_spec(one)?)),
                    _ => Err(Error::UnsupportedFunctionCall(format!(
                        "{name}() expects exactly one column argument"
                    ))),
                }
            }
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "{SELECT_AGG_EXPECTED}, got {expr:?}"
            ))),
        },
        Expr::Literal(Literal::Symbol(s)) => {
            Ok(SelectItem::column(ColumnSpec::CaseInsensitive(s.clone())))
        }
        Expr::Literal(Literal::String(s)) => Ok(SelectItem::column(ColumnSpec::Exact(s.clone()))),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "{SELECT_AGG_EXPECTED}, got {expr:?}"
        ))),
    }
}

/// Extracts select items: column refs or `sum(column)` / `avg(column)` / `min(column)` / `max(column)` /
/// `count(column)` / `count_distinct(column)` from the positional arguments, then aliased items
/// from `name: value` / `"quoted name": value` keyword arguments.
pub(super) fn extract_select_items(call: &FunctionCall) -> crate::Result<Vec<SelectItem>> {
    let mut items = call
        .positional_args
        .iter()
        .map(select_item_from_expr)
        .collect::<crate::Result<Vec<_>>>()?;
    for (alias, value) in &call.keyword_args {
        items.push(select_item_from_expr(value)?.with_alias(alias.clone()));
    }
    Ok(items)
}

/// Extracts `group_by(...)` keys, plus aliases from `name: value` / `"quoted name": value`
/// keyword arguments. A key's alias becomes the default output name for that column in
/// `select()`, unless `select()` gives its own alias.
pub(super) fn extract_group_by_keys(call: &FunctionCall) -> crate::Result<Vec<GroupByKey>> {
    let mut keys = call
        .positional_args
        .iter()
        .map(|e| Ok(GroupByKey::new(extract_one_column_spec(e)?)))
        .collect::<crate::Result<Vec<_>>>()?;
    for (alias, value) in &call.keyword_args {
        keys.push(GroupByKey::new(extract_one_column_spec(value)?).with_alias(alias.clone()));
    }
    Ok(keys)
}

#[cfg(test)]
pub(super) fn is_head_call(expr: Option<&Expr>) -> bool {
    expr.and_then(call_name) == Some("head")
}

/// Plans a single pipeline stage from an AST expression.
pub(super) fn plan_stage(expr: Expr) -> crate::Result<ReplPipelineStage> {
    let Expr::FunctionCall(call) = expr else {
        return Err(Error::UnsupportedExpression(expr.to_string()));
    };
    match call.name.0.as_str() {
        "read" => {
            let path = extract_path_from_args("read", positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Read { path })
        }
        "filter" => {
            let sql = extract_sql_predicate_from_args("filter", positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Filter { sql })
        }
        "group_by" => {
            let columns = extract_group_by_keys(&call)?;
            if columns.is_empty() {
                return Err(Error::UnsupportedFunctionCall(
                    "group_by expects at least one column".to_string(),
                ));
            }
            Ok(ReplPipelineStage::GroupBy { columns })
        }
        "select" => {
            let columns = extract_select_items(&call)?;
            if columns.is_empty() {
                return Err(Error::UnsupportedFunctionCall(
                    "select expects at least one column name".to_string(),
                ));
            }
            Ok(ReplPipelineStage::Select { columns })
        }
        "head" => {
            let n = extract_head_n(positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Head { n })
        }
        "tail" => {
            let n = extract_tail_n(positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Tail { n })
        }
        "sample" => {
            let n = extract_sample_n(positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Sample { n })
        }
        "count" => {
            if !positional_args_only(&call)?.is_empty() {
                return Err(Error::UnsupportedFunctionCall(
                    "count takes no arguments".to_string(),
                ));
            }
            Ok(ReplPipelineStage::Count)
        }
        "schema" => {
            if !positional_args_only(&call)?.is_empty() {
                return Err(Error::UnsupportedFunctionCall(
                    "schema takes no arguments".to_string(),
                ));
            }
            Ok(ReplPipelineStage::Schema)
        }
        "write" => {
            let path = extract_path_from_args("write", positional_args_only(&call)?)?;
            Ok(ReplPipelineStage::Write { path })
        }
        _ => Err(Error::UnsupportedFunctionCall(call.name.0.clone())),
    }
}

/// Plans a full pipeline and returns whether the statement is incomplete
/// (the final explicit stage is non-terminal).
pub(super) fn plan_pipeline_with_state(
    exprs: Vec<Expr>,
) -> crate::Result<(Vec<ReplPipelineStage>, bool)> {
    // Collect all stages from the expressions.
    let mut stages: Vec<ReplPipelineStage> = exprs
        .into_iter()
        .map(plan_stage)
        .collect::<crate::Result<Vec<_>>>()?;
    // Check if the statement is incomplete (before implicit print).
    let statement_incomplete = if repl_pipeline_last_select_is_terminal(&stages) {
        false
    } else {
        stages
            .last()
            .is_some_and(ReplPipelineStage::is_non_terminal)
    };
    // Add any implicit followup stage.
    if let Some(implicit_stage) = stages
        .last()
        .and_then(ReplPipelineStage::get_implicit_followup_stage)
    {
        stages.push(implicit_stage);
    }
    Ok((stages, statement_incomplete))
}

fn validate_grouped_select(keys: &[GroupByKey], items: &[SelectItem]) -> crate::Result<()> {
    for key in keys {
        let mut found = false;
        for item in items {
            if let SelectItem::Column(c, _) = item
                && key.matches_select_column(c)
            {
                found = true;
                break;
            }
        }
        if !found {
            return Err(Error::InvalidReplPipeline(
                "every group_by column must appear in select() as a plain column".to_string(),
            ));
        }
    }
    for item in items {
        match item {
            SelectItem::Column(c, _) => {
                if !keys.iter().any(|k| k.matches_select_column(c)) {
                    return Err(Error::InvalidReplPipeline(
                        "select with group_by: non-key columns must use an aggregate (sum, avg, min, max, count, or count_distinct), not plain columns"
                            .to_string(),
                    ));
                }
            }
            SelectItem::Sum(..)
            | SelectItem::Avg(..)
            | SelectItem::Min(..)
            | SelectItem::Max(..)
            | SelectItem::Count(..)
            | SelectItem::CountDistinct(..) => {}
        }
    }
    Ok(())
}

/// Validates that stages match `read` → optional permuted `filter` (at most two, straddling `select` if two) / `group_by` / `select` (at most one each) → optional slice or `schema`/`count` → optional `write`,
/// with optional trailing `print` only after head/tail/sample. A single `filter` maps to before or after `select` by stage order; two `filter` stages require `select(...)` strictly between them (WHERE-like then HAVING-like when aggregating).
pub(super) fn validate_repl_pipeline_stages(stages: &[ReplPipelineStage]) -> crate::Result<()> {
    if stages.is_empty() {
        return Err(Error::InvalidReplPipeline("empty pipeline".to_string()));
    }

    let body = match stages.last() {
        Some(ReplPipelineStage::Print) if stages.len() >= 2 => &stages[..stages.len() - 1],
        _ => stages,
    };

    if !matches!(body.first(), Some(ReplPipelineStage::Read { .. })) {
        return Err(Error::InvalidReplPipeline(
            "pipeline must start with read(path)".to_string(),
        ));
    }

    let mut i = 1usize;
    let mut filter_indices: Vec<usize> = Vec::new();
    let mut select_idx: Option<usize> = None;
    let mut group_by_cols: Option<&Vec<GroupByKey>> = None;
    let mut select_items: Option<&Vec<SelectItem>> = None;

    while i < body.len() {
        match body.get(i) {
            Some(ReplPipelineStage::Filter { .. }) => {
                if filter_indices.len() >= 2 {
                    return Err(Error::InvalidReplPipeline(
                        "at most two filter(...) stages are allowed in a pipeline".to_string(),
                    ));
                }
                filter_indices.push(i);
                i += 1;
            }
            Some(ReplPipelineStage::GroupBy { columns }) => {
                if group_by_cols.is_some() {
                    return Err(Error::InvalidReplPipeline(
                        "only one group_by(...) is allowed in a pipeline".to_string(),
                    ));
                }
                group_by_cols = Some(columns);
                i += 1;
            }
            Some(ReplPipelineStage::Select { columns }) => {
                if select_items.is_some() {
                    return Err(Error::InvalidReplPipeline(
                        "only one select(...) is allowed in a pipeline".to_string(),
                    ));
                }
                if select_idx.is_none() {
                    select_idx = Some(i);
                }
                select_items = Some(columns);
                i += 1;
            }
            Some(
                ReplPipelineStage::Head { .. }
                | ReplPipelineStage::Tail { .. }
                | ReplPipelineStage::Sample { .. }
                | ReplPipelineStage::Schema
                | ReplPipelineStage::Count
                | ReplPipelineStage::Write { .. },
            ) => break,
            Some(ReplPipelineStage::Read { .. }) => {
                return Err(Error::InvalidReplPipeline(
                    "unexpected read(path) after start of pipeline".to_string(),
                ));
            }
            Some(ReplPipelineStage::Print) => {
                return Err(Error::InvalidReplPipeline(
                    "unexpected print() in pipeline body".to_string(),
                ));
            }
            None => break,
        }
    }

    if group_by_cols.is_some() && select_items.is_none() {
        return Err(Error::InvalidReplPipeline(
            "group_by(...) requires select(...)".to_string(),
        ));
    }

    if filter_indices.len() == 2 {
        let Some(si) = select_idx else {
            return Err(Error::InvalidReplPipeline(
                "two filter(...) stages require select(...) between them (one before and one after select(...))"
                    .to_string(),
            ));
        };
        let f0 = filter_indices[0].min(filter_indices[1]);
        let f1 = filter_indices[0].max(filter_indices[1]);
        if !(f0 < si && si < f1) {
            return Err(Error::InvalidReplPipeline(
                "two filter(...) stages must have select(...) strictly between them (one before select, one after)"
                    .to_string(),
            ));
        }
    }

    if let Some(keys) = group_by_cols {
        let items = select_items.expect("select when group_by");
        validate_grouped_select(keys, items)?;
    } else if let Some(items) = select_items {
        let spec = SelectSpec {
            columns: items.to_vec(),
            group_by: None,
        };
        if spec.has_aggregates() && !spec.is_aggregate_only() {
            return Err(Error::InvalidReplPipeline(
                "mixed column projections and aggregates in select require group_by(); \
                 put every group key in group_by() and list them as columns in select()"
                    .to_string(),
            ));
        }
    }

    match body.get(i) {
        Some(
            ReplPipelineStage::Head { .. }
            | ReplPipelineStage::Tail { .. }
            | ReplPipelineStage::Sample { .. },
        ) => {
            i += 1;
            if matches!(
                body.get(i),
                Some(
                    ReplPipelineStage::Head { .. }
                        | ReplPipelineStage::Tail { .. }
                        | ReplPipelineStage::Sample { .. },
                ),
            ) {
                return Err(Error::InvalidReplPipeline(
                    "only one of head(...), tail(...), or sample(...) is allowed".to_string(),
                ));
            }
            if matches!(body.get(i), Some(ReplPipelineStage::Write { .. })) {
                i += 1;
            }
        }
        Some(ReplPipelineStage::Schema) | Some(ReplPipelineStage::Count) => {
            i += 1;
        }
        Some(ReplPipelineStage::Write { .. }) => {
            i += 1;
        }
        None => {
            if !repl_pipeline_last_select_is_terminal(body) {
                return Err(Error::InvalidReplPipeline(
                    "pipeline must end with write, head, tail, sample, schema, count, or a complete grouped select()"
                        .to_string(),
                ));
            }
        }
        _ => {
            return Err(Error::InvalidReplPipeline(
                "invalid pipeline stage order (expected read, optional filter/group_by/select in any order, then head|tail|sample|schema|count, or write)".to_string(),
            ));
        }
    }

    if i != body.len() {
        return Err(Error::InvalidReplPipeline(
            "invalid pipeline stage order (expected read, optional filter/group_by/select, optional head|tail|sample|schema|count, optional write)".to_string(),
        ));
    }

    if matches!(stages.last(), Some(ReplPipelineStage::Print)) {
        if !matches!(
            body.last(),
            Some(
                ReplPipelineStage::Head { .. }
                    | ReplPipelineStage::Tail { .. }
                    | ReplPipelineStage::Sample { .. },
            )
        ) {
            return Err(Error::InvalidReplPipeline(
                "print may only follow head, tail, or sample".to_string(),
            ));
        }
    } else {
        let ends_ok = match body.last() {
            Some(ReplPipelineStage::Write { .. })
            | Some(
                ReplPipelineStage::Head { .. }
                | ReplPipelineStage::Tail { .. }
                | ReplPipelineStage::Sample { .. },
            )
            | Some(ReplPipelineStage::Schema)
            | Some(ReplPipelineStage::Count) => true,
            _ => repl_pipeline_last_select_is_terminal(body),
        };
        if !ends_ok {
            return Err(Error::InvalidReplPipeline(
                "pipeline must end with write, head, tail, sample, schema, count, or a complete grouped select()"
                    .to_string(),
            ));
        }
    }

    Ok(())
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
pub(super) fn extract_head_n(args: &[Expr]) -> crate::Result<usize> {
    extract_usize_arg("head", args)
}

/// Extracts the integer argument N from a tail() call's args.
pub(super) fn extract_tail_n(args: &[Expr]) -> crate::Result<usize> {
    extract_usize_arg("tail", args)
}

/// Extracts the optional integer argument N from a sample() call's args (default 10).
pub(super) fn extract_sample_n(args: &[Expr]) -> crate::Result<usize> {
    if args.is_empty() {
        return Ok(10);
    }
    extract_usize_arg("sample", args)
}
