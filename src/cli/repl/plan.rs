//! REPL statement completeness (which accumulated lines form one pipeline) and pipeline stage
//! validation.

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::FunctionCall;

use super::stage::ReplPipelineStage;
use super::stage::repl_pipeline_last_select_is_terminal;
use crate::Error;
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

/// The called function's name, if `expr` is a function call.
fn call_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::FunctionCall(call) => Some(call.name.0.as_str()),
        _ => None,
    }
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
