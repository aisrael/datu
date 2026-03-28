//! Parse flt [`Expr`] trees into [`PipelineStage`] lists and validate order.

use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;

use super::stage::PipelineStage;
use crate::Error;
use crate::pipeline::ColumnSpec;

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
    if let Expr::FunctionCall(name, _) = expr {
        matches!(
            name.to_string().as_str(),
            "count" | "head" | "tail" | "sample" | "schema" | "write"
        )
    } else {
        false
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

/// Extracts column specs from select args. Symbols (:one) and identifiers use case-insensitive
/// match; strings ("one") use exact match.
pub(super) fn extract_column_specs(args: &[Expr]) -> crate::Result<Vec<ColumnSpec>> {
    args.iter()
        .map(|expr| match expr {
            Expr::Literal(Literal::Symbol(s)) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
            Expr::Literal(Literal::String(s)) => Ok(ColumnSpec::Exact(s.clone())),
            Expr::Ident(s) => Ok(ColumnSpec::CaseInsensitive(s.clone())),
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "select expects symbol or string column names, got {expr:?}"
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
pub(super) fn plan_stage(expr: Expr) -> crate::Result<PipelineStage> {
    match expr {
        Expr::FunctionCall(name, args) => {
            let name_str = name.to_string();
            match name_str.as_str() {
                "read" => {
                    let path = extract_path_from_args("read", &args)?;
                    Ok(PipelineStage::Read { path })
                }
                "select" => {
                    let columns = extract_column_specs(&args)?;
                    if columns.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "select expects at least one column name".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Select { columns })
                }
                "head" => {
                    let n = extract_head_n(&args)?;
                    Ok(PipelineStage::Head { n })
                }
                "tail" => {
                    let n = extract_tail_n(&args)?;
                    Ok(PipelineStage::Tail { n })
                }
                "sample" => {
                    let n = extract_sample_n(&args)?;
                    Ok(PipelineStage::Sample { n })
                }
                "count" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "count takes no arguments".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Count)
                }
                "schema" => {
                    if !args.is_empty() {
                        return Err(Error::UnsupportedFunctionCall(
                            "schema takes no arguments".to_string(),
                        ));
                    }
                    Ok(PipelineStage::Schema)
                }
                "write" => {
                    let path = extract_path_from_args("write", &args)?;
                    Ok(PipelineStage::Write { path })
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
) -> crate::Result<(Vec<PipelineStage>, bool)> {
    let mut stages: Vec<PipelineStage> = exprs
        .into_iter()
        .map(plan_stage)
        .collect::<crate::Result<Vec<_>>>()?;
    let statement_incomplete = stages.last().is_some_and(PipelineStage::is_non_terminal);
    if let Some(implicit_stage) = stages
        .last()
        .and_then(PipelineStage::get_implicit_followup_stage)
    {
        stages.push(implicit_stage);
    }
    Ok((stages, statement_incomplete))
}

/// Validates that stages match `read` → optional `select` → optional slice or `schema`/`count` → optional `write`,
/// with optional trailing `print` only after head/tail/sample.
pub(super) fn validate_repl_pipeline_stages(stages: &[PipelineStage]) -> crate::Result<()> {
    if stages.is_empty() {
        return Err(Error::InvalidReplPipeline("empty pipeline".to_string()));
    }

    let body = match stages.last() {
        Some(PipelineStage::Print) if stages.len() >= 2 => &stages[..stages.len() - 1],
        _ => stages,
    };

    if !matches!(body.first(), Some(PipelineStage::Read { .. })) {
        return Err(Error::InvalidReplPipeline(
            "pipeline must start with read(path)".to_string(),
        ));
    }

    let mut i = 1usize;
    if matches!(body.get(i), Some(PipelineStage::Select { .. })) {
        i += 1;
    }
    if matches!(body.get(i), Some(PipelineStage::Select { .. })) {
        return Err(Error::InvalidReplPipeline(
            "only one select(...) is allowed in a pipeline".to_string(),
        ));
    }

    match body.get(i) {
        Some(
            PipelineStage::Head { .. } | PipelineStage::Tail { .. } | PipelineStage::Sample { .. },
        ) => {
            i += 1;
            if matches!(
                body.get(i),
                Some(
                    PipelineStage::Head { .. }
                        | PipelineStage::Tail { .. }
                        | PipelineStage::Sample { .. },
                ),
            ) {
                return Err(Error::InvalidReplPipeline(
                    "only one of head(...), tail(...), or sample(...) is allowed".to_string(),
                ));
            }
            if matches!(body.get(i), Some(PipelineStage::Write { .. })) {
                i += 1;
            }
        }
        Some(PipelineStage::Schema) | Some(PipelineStage::Count) => {
            i += 1;
        }
        Some(PipelineStage::Write { .. }) => {
            i += 1;
        }
        None => {
            return Err(Error::InvalidReplPipeline(
                "pipeline must end with write, head, tail, sample, schema, or count".to_string(),
            ));
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

    if matches!(stages.last(), Some(PipelineStage::Print)) {
        if !matches!(
            body.last(),
            Some(
                PipelineStage::Head { .. }
                    | PipelineStage::Tail { .. }
                    | PipelineStage::Sample { .. },
            )
        ) {
            return Err(Error::InvalidReplPipeline(
                "print may only follow head, tail, or sample".to_string(),
            ));
        }
    } else {
        match body.last() {
            Some(PipelineStage::Write { .. })
            | Some(
                PipelineStage::Head { .. }
                | PipelineStage::Tail { .. }
                | PipelineStage::Sample { .. },
            )
            | Some(PipelineStage::Schema)
            | Some(PipelineStage::Count) => {}
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
