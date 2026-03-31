//! Parse flt [`Expr`] trees into [`PipelineStage`] lists and validate order.

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;

use super::stage::ReplPipelineStage;
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

pub(super) fn is_terminal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall(name, args) => match name.to_string().as_str() {
            "count" | "head" | "tail" | "sample" | "schema" | "write" => true,
            "select" => select_args_are_all_aggregates(args),
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
                    if n.to_string().as_str() == "sum" && a.len() == 1
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

/// Extracts select items: column refs or `sum(column)`.
pub(super) fn extract_select_items(args: &[Expr]) -> crate::Result<Vec<SelectItem>> {
    args.iter()
        .map(|expr| match expr {
            Expr::FunctionCall(name, inner) if name.to_string().as_str() == "sum" => {
                match inner.as_slice() {
                    [one] => Ok(SelectItem::Sum(extract_one_column_spec(one)?)),
                    _ => Err(Error::UnsupportedFunctionCall(
                        "sum() expects exactly one column argument".to_string(),
                    )),
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
                "select expects column names or sum(column), got {expr:?}"
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
    // Check if the statement is incomplete.
    let statement_incomplete = stages
        .last()
        .is_some_and(ReplPipelineStage::is_non_terminal);
    // Add any implicit followup stage.
    if let Some(implicit_stage) = stages
        .last()
        .and_then(ReplPipelineStage::get_implicit_followup_stage)
    {
        stages.push(implicit_stage);
    }
    Ok((stages, statement_incomplete))
}

/// Validates that stages match `read` → optional `select` → optional slice or `schema`/`count` → optional `write`,
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
    if matches!(body.get(i), Some(ReplPipelineStage::Select { .. })) {
        i += 1;
    }
    if matches!(body.get(i), Some(ReplPipelineStage::Select { .. })) {
        return Err(Error::InvalidReplPipeline(
            "only one select(...) is allowed in a pipeline".to_string(),
        ));
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
            if !(body.len() == 2 && body.get(1).is_some_and(repl_select_is_aggregate_only)) {
                return Err(Error::InvalidReplPipeline(
                    "pipeline must end with write, head, tail, sample, schema, or count"
                        .to_string(),
                ));
            }
        }
        _ => {
            return Err(Error::InvalidReplPipeline(
                "invalid pipeline stage order (expected read, optional select, then head|tail|sample|schema|count, or write)".to_string(),
            ));
        }
    }

    if i != body.len() {
        return Err(Error::InvalidReplPipeline(
            "invalid pipeline stage order (expected read, optional select, optional head|tail|sample|schema|count, optional write)".to_string(),
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
        match body.last() {
            Some(ReplPipelineStage::Write { .. })
            | Some(
                ReplPipelineStage::Head { .. }
                | ReplPipelineStage::Tail { .. }
                | ReplPipelineStage::Sample { .. },
            )
            | Some(ReplPipelineStage::Schema)
            | Some(ReplPipelineStage::Count) => {}
            Some(stage @ ReplPipelineStage::Select { .. })
                if repl_select_is_aggregate_only(stage) => {}
            _ => {
                return Err(Error::InvalidReplPipeline(
                    "pipeline must end with write, head, tail, sample, schema, or count"
                        .to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn repl_select_is_aggregate_only(stage: &ReplPipelineStage) -> bool {
    matches!(
        stage,
        ReplPipelineStage::Select { columns }
            if SelectSpec {
                columns: columns.clone(),
            }
            .is_aggregate_only()
    )
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
