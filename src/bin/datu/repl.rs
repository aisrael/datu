//! REPL for datu

use anyhow::Result;
use datu::Error;
use datu::FileType;
use datu::pipeline::RecordBatchReaderSource;
use datu::pipeline::select;
use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::ast::Literal;
use flt::parser::parse_expr;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::commands::convert;

/// Maximum number of inputs to keep in REPL history.
const HISTORY_DEPTH: usize = 1000;

/// Context for a REPL session.
pub struct ReplContext {
    pub reader: Option<RecordBatchReaderSource>,
    pub writer: Option<String>,
}

/// Runs the datu REPL.
pub async fn run() -> Result<()> {
    let config = rustyline::Config::builder()
        .max_history_size(HISTORY_DEPTH)?
        .auto_add_history(true)
        .build();
    let mut rl = DefaultEditor::with_config(config)?;
    let mut context = ReplContext {
        reader: None,
        writer: None,
    };
    loop {
        let line = match rl.readline("> ") {
            Ok(line) => line,
            Err(ReadlineError::Eof) => break Ok(()),
            Err(ReadlineError::Interrupted) => continue,
            Err(e) => return Err(e.into()),
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match parse_expr(line) {
            Ok((remainder, expr)) => {
                let remainder = remainder.trim();
                if remainder.is_empty() {
                    eval(&mut context, expr).await?;
                } else {
                    eprintln!(
                        "parse error: unexpected input after expression: {:?}",
                        remainder
                    );
                }
            }
            Err(e) => {
                eprintln!("parse error: {:?}", e);
            }
        }
    }
}

/// Evaluates a datu expression.
async fn eval(context: &mut ReplContext, expr: Expr) -> datu::Result<()> {
    match expr {
        Expr::BinaryExpr(left, op, right) => {
            eval_binary_expr(context, left, op, right).await?;
        }
        _ => return Err(Error::UnsupportedExpression(expr.to_string())),
    }
    Ok(())
}

/// Evaluates a binary expression.
async fn eval_binary_expr(
    context: &mut ReplContext,
    left: Box<Expr>,
    op: BinaryOp,
    right: Box<Expr>,
) -> datu::Result<()> {
    match op {
        BinaryOp::Pipe => {
            eval_pipe(context, left, right).await?;
        }
        _ => return Err(Error::UnsupportedOperator(op.to_string())),
    }
    Ok(())
}

/// Collects pipeline stages from a pipe expression (e.g. a |> b |> c -> [a, b, c]).
fn collect_pipe_stages(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryExpr(l, BinaryOp::Pipe, r) => {
            collect_pipe_stages(*l, out);
            collect_pipe_stages(*r, out);
        }
        other => out.push(other),
    }
}

/// Evaluates a pipe expression.
async fn eval_pipe(
    context: &mut ReplContext,
    left: Box<Expr>,
    right: Box<Expr>,
) -> datu::Result<()> {
    let mut stages = Vec::new();
    collect_pipe_stages(*left, &mut stages);
    collect_pipe_stages(*right, &mut stages);
    for stage in stages {
        eval_stage(context, stage).await?;
    }
    Ok(())
}

/// Evaluates a single pipeline stage (read, select, or write).
async fn eval_stage(context: &mut ReplContext, expr: Expr) -> datu::Result<()> {
    match expr {
        Expr::FunctionCall(name, args) => {
            let name_str = name.to_string();
            match name_str.as_str() {
                "read" => eval_read(context, args)?,
                "select" => eval_select(context, args).await?,
                "write" => eval_write(context, args)?,
                _ => return Err(Error::UnsupportedFunctionCall(name_str)),
            }
        }
        _ => return Err(Error::UnsupportedExpression(expr.to_string())),
    }
    Ok(())
}

/// Evaluates a read function call.
fn eval_read(context: &mut ReplContext, args: Vec<Expr>) -> datu::Result<()> {
    let path = match args.as_slice() {
        [Expr::Literal(Literal::String(s))] => s.clone(),
        _ => {
            return Err(Error::UnsupportedFunctionCall(format!(
                "read expects a single string argument, got {args:?}"
            )));
        }
    };
    let file_type: FileType = path.as_str().try_into()?;
    let convert_args = convert::ConvertArgs {
        input: path,
        output: String::new(),
        select: None,
        limit: None,
        sparse: true,
        json_pretty: false,
    };
    let reader = convert::get_reader_step(file_type, &convert_args)
        .map_err(|e| Error::GenericError(e.to_string()))?;
    context.reader = Some(reader);
    Ok(())
}

/// Extracts column names from select args (symbols like :one, identifiers, or strings like "one").
fn extract_column_names(args: &[Expr]) -> datu::Result<Vec<String>> {
    args.iter()
        .map(|expr| match expr {
            Expr::Literal(Literal::Symbol(s)) => Ok(s.clone()),
            Expr::Literal(Literal::String(s)) => Ok(s.clone()),
            Expr::Ident(s) => Ok(s.clone()),
            _ => Err(Error::UnsupportedFunctionCall(format!(
                "select expects symbol or string column names, got {expr:?}"
            ))),
        })
        .collect()
}

/// Evaluates a select function call using the DataFusion DataFrame API.
async fn eval_select(context: &mut ReplContext, args: Vec<Expr>) -> datu::Result<()> {
    let columns = extract_column_names(&args)?;
    if columns.is_empty() {
        return Err(Error::UnsupportedFunctionCall(
            "select expects at least one column name".to_string(),
        ));
    }
    let reader = context.reader.take().ok_or_else(|| {
        Error::GenericError("select requires a preceding read in the pipe".to_string())
    })?;
    let mut source = reader;
    let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    {
        let r = source.get()?;
        for batch in r {
            let b = batch.map_err(|e| Error::GenericError(e.to_string()))?;
            batches.push(b);
        }
    }
    let selected = select::select_columns_from_batches(batches, &columns).await?;
    context.reader = Some(selected);
    Ok(())
}

/// Evaluates a write function call.
fn eval_write(context: &mut ReplContext, args: Vec<Expr>) -> datu::Result<()> {
    let output_path = match args.as_slice() {
        [Expr::Literal(Literal::String(s))] => s.clone(),
        _ => {
            return Err(Error::UnsupportedFunctionCall(format!(
                "write expects a single string argument, got {args:?}"
            )));
        }
    };
    let reader = context.reader.take().ok_or_else(|| {
        Error::GenericError("write requires a preceding read in the pipe".to_string())
    })?;
    let output_file_type: FileType = output_path.as_str().try_into()?;
    let convert_args = convert::ConvertArgs {
        input: String::new(),
        output: output_path.clone(),
        select: None,
        limit: None,
        sparse: true,
        json_pretty: false,
    };
    convert::get_writer_step(reader, output_file_type, &convert_args, true)
        .map_err(|e| Error::GenericError(e.to_string()))?;
    context.writer = Some(output_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_repl_pipeline_read_select_write() {
        // Test: read("fixtures/table.parquet") |> select(:one, :two) |> write("${TMP}/table.avro")
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("table.avro");
        let output_str = output_path.to_str().expect("path to str").to_string();

        let pipeline = format!(
            r#"read("fixtures/table.parquet") |> select(:one, :two) |> write("{}")"#,
            output_str.replace('\\', "\\\\")
        );
        let (remainder, expr) = flt::parser::parse_expr(&pipeline).expect("parse");
        assert!(remainder.trim().is_empty(), "unconsumed: {:?}", remainder);

        let mut context = ReplContext {
            reader: None,
            writer: None,
        };
        eval(&mut context, expr).await.expect("eval");

        assert!(output_path.exists());
    }
}
