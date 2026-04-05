//! Parse flt [`Expr`] trees into [`PipelineStage`] lists and validate order.

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;

use super::stage::ReplPipelineStage;
use super::stage::repl_pipeline_last_select_is_terminal;
use crate::Error;
use crate::pipeline::ColumnSpec;
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

fn expr_is_group_by_call(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::FunctionCall(name, _) if name.to_string().as_str() == "group_by"
    )
}

fn expr_is_select_call(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::FunctionCall(name, _) if name.to_string().as_str() == "select"
    )
}

/// True when the accumulated pipe stages form a complete REPL statement (flush and execute).
pub(super) fn is_statement_complete(pending_exprs: &[Expr]) -> bool {
    let Some(last) = pending_exprs.last() else {
        return false;
    };
    match last {
        Expr::FunctionCall(name, args) => match name.to_string().as_str() {
            "count" | "head" | "tail" | "sample" | "schema" | "write" => true,
            "group_by" => pending_exprs[..pending_exprs.len().saturating_sub(1)]
                .iter()
                .any(expr_is_select_call),
            "select" => {
                if select_args_are_all_aggregates(args) {
                    return true;
                }
                pending_exprs[..pending_exprs.len().saturating_sub(1)]
                    .iter()
                    .any(expr_is_group_by_call)
            }
            _ => false,
        },
        _ => false,
    }
}

fn select_args_are_all_aggregates(args: &[Expr]) -> bool {
    !args.is_empty()
        && args.iter().all(|e| {
            matches!(
                e,
                Expr::FunctionCall(n, a)
                    if matches!(
                        n.to_string().as_str(),
                        "sum" | "avg" | "min" | "max" | "count" | "count_distinct"
                    ) && a.len() == 1
            )
        })
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

fn extract_one_column_spec(expr: &Expr) -> crate::Result<ColumnSpec> {
    match expr {
        Expr::Literal(Literal::Symbol(s)) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
        Expr::Literal(Literal::String(s)) => Ok(ColumnSpec::Exact(s.clone())),
        Expr::Ident(s) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
        _ => Err(Error::UnsupportedFunctionCall(format!(
            "expected a column (symbol, string, or identifier), got {expr:?}"
        ))),
    }
}

fn select_aggregate_item(name: &str, col: ColumnSpec) -> SelectItem {
    match name {
        "sum" => SelectItem::Sum(col),
        "avg" => SelectItem::Avg(col),
        "min" => SelectItem::Min(col),
        "max" => SelectItem::Max(col),
        "count" => SelectItem::Count(col),
        "count_distinct" => SelectItem::CountDistinct(col),
        _ => unreachable!(
            "select_aggregate_item only called for sum, avg, min, max, count, or count_distinct"
        ),
    }
}

/// Extracts select items: column refs or `sum(column)` / `avg(column)` / `min(column)` / `max(column)` /
/// `count(column)` / `count_distinct(column)`.
pub(super) fn extract_select_items(args: &[Expr]) -> crate::Result<Vec<SelectItem>> {
    const SELECT_AGG_EXPECTED: &str = "select expects column names, sum(column), avg(column), min(column), max(column), count(column), or count_distinct(column)";
    args.iter()
        .map(|expr| match expr {
            Expr::FunctionCall(name, inner) => {
                let name_str = name.to_string();
                match name_str.as_str() {
                    "sum" | "avg" | "min" | "max" | "count" | "count_distinct" => {
                        match inner.as_slice() {
                            [one] => Ok(select_aggregate_item(
                                name_str.as_str(),
                                extract_one_column_spec(one)?,
                            )),
                            _ => Err(Error::UnsupportedFunctionCall(format!(
                                "{name_str}() expects exactly one column argument"
                            ))),
                        }
                    }
                    _ => Err(Error::UnsupportedFunctionCall(format!(
                        "{SELECT_AGG_EXPECTED}, got {expr:?}"
                    ))),
                }
            }
            Expr::Literal(Literal::Symbol(s)) => {
                Ok(SelectItem::Column(ColumnSpec::CaseInsensitive(s.clone())))
            }
            Expr::Literal(Literal::String(s)) => {
                Ok(SelectItem::Column(ColumnSpec::Exact(s.clone())))
            }
            Expr::Ident(s) => Ok(SelectItem::Column(ColumnSpec::CaseInsensitive(s.clone()))),
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "{SELECT_AGG_EXPECTED}, got {expr:?}"
            ))),
        })
        .collect()
}

#[cfg(test)]
pub(super) fn is_head_call(expr: Option<&Expr>) -> bool {
    if let Some(Expr::FunctionCall(name, _)) = expr {
        *name == "head"
    } else {
        false
    }
}

/// Plans a single pipeline stage from an AST expression.
pub(super) fn plan_stage(expr: Expr) -> crate::Result<ReplPipelineStage> {
    match expr {
        Expr::FunctionCall(name, args) => {
            let name_str = name.to_string();
            match name_str.as_str() {
                "read" => {
                    let path = extract_path_from_args("read", &args)?;
                    Ok(ReplPipelineStage::Read { path })
                }
                "group_by" => {
                    if args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "group_by expects at least one column".to_string(),
                        ));
                    }
                    let columns = args
                        .iter()
                        .map(extract_one_column_spec)
                        .collect::<crate::Result<Vec<_>>>()?;
                    Ok(ReplPipelineStage::GroupBy { columns })
                }
                "select" => {
                    let columns = extract_select_items(&args)?;
                    if columns.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "select expects at least one column name".to_string(),
                        ));
                    }
                    Ok(ReplPipelineStage::Select { columns })
                }
                "head" => {
                    let n = extract_head_n(&args)?;
                    Ok(ReplPipelineStage::Head { n })
                }
                "tail" => {
                    let n = extract_tail_n(&args)?;
                    Ok(ReplPipelineStage::Tail { n })
                }
                "sample" => {
                    let n = extract_sample_n(&args)?;
                    Ok(ReplPipelineStage::Sample { n })
                }
                "count" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "count takes no arguments".to_string(),
                        ));
                    }
                    Ok(ReplPipelineStage::Count)
                }
                "schema" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "schema takes no arguments".to_string(),
                        ));
                    }
                    Ok(ReplPipelineStage::Schema)
                }
                "write" => {
                    let path = extract_path_from_args("write", &args)?;
                    Ok(ReplPipelineStage::Write { path })
                }
                _ => Err(Error::UnsupportedFunctionCall(name_str)),
            }
        }
        _ => Err(Error::UnsupportedExpression(expr.to_string())),
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

fn validate_grouped_select(keys: &[ColumnSpec], items: &[SelectItem]) -> crate::Result<()> {
    for key in keys {
        let mut found = false;
        for item in items {
            if let SelectItem::Column(c) = item
                && c == key
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
            SelectItem::Column(c) => {
                if !keys.iter().any(|k| k == c) {
                    return Err(Error::InvalidReplPipeline(
                        "select with group_by: non-key columns must use an aggregate (sum, avg, min, max, count, or count_distinct), not plain columns"
                            .to_string(),
                    ));
                }
            }
            SelectItem::Sum(_)
            | SelectItem::Avg(_)
            | SelectItem::Min(_)
            | SelectItem::Max(_)
            | SelectItem::Count(_)
            | SelectItem::CountDistinct(_) => {}
        }
    }
    Ok(())
}

/// Validates that stages match `read` → optional `group_by` / `select` (either order) → optional slice or `schema`/`count` → optional `write`,
/// with optional trailing `print` only after head/tail/sample.
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
    let mut group_by_cols: Option<&Vec<ColumnSpec>> = None;
    let mut select_items: Option<&Vec<SelectItem>> = None;

    for _ in 0..2 {
        match body.get(i) {
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
                select_items = Some(columns);
                i += 1;
            }
            _ => break,
        }
    }

    if group_by_cols.is_some() && select_items.is_none() {
        return Err(Error::InvalidReplPipeline(
            "group_by(...) requires select(...)".to_string(),
        ));
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
                "invalid pipeline stage order (expected read, optional group_by and select, then head|tail|sample|schema|count, or write)".to_string(),
            ));
        }
    }

    if i != body.len() {
        return Err(Error::InvalidReplPipeline(
            "invalid pipeline stage order (expected read, optional group_by/select, optional head|tail|sample|schema|count, optional write)".to_string(),
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
