//! REPL for datu

use anyhow::Result;
use datu::Error;
use datu::FileType;
use datu::pipeline::RecordBatchReaderSource;
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
pub fn run() -> Result<()> {
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
                    eval(&mut context, expr)?;
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
fn eval(context: &mut ReplContext, expr: Expr) -> datu::Result<()> {
    println!("{:?}", expr);
    match expr {
        Expr::BinaryExpr(left, op, right) => {
            eval_binary_expr(context, left, op, right)?;
        }
        _ => return Err(Error::UnsupportedExpression(expr.to_string())),
    }
    Ok(())
}

/// Evaluates a binary expression.
fn eval_binary_expr(
    context: &mut ReplContext,
    left: Box<Expr>,
    op: BinaryOp,
    right: Box<Expr>,
) -> datu::Result<()> {
    match op {
        BinaryOp::Pipe => {
            eval_pipe(context, left, right)?;
        }
        _ => return Err(Error::UnsupportedOperator(op.to_string())),
    }
    Ok(())
}

/// Evaluates a pipe expression.
fn eval_pipe(context: &mut ReplContext, left: Box<Expr>, right: Box<Expr>) -> datu::Result<()> {
    println!("left: {left:?}");
    println!("right: {right:?}");
    match *left {
        Expr::FunctionCall(name, args) => {
            if is_supported_function_call(&name) {
                if name == "read" {
                    eval_read(context, args)?;
                } else {
                    return Err(Error::UnsupportedFunctionCall(name.to_string()));
                }
            } else {
                return Err(Error::UnsupportedFunctionCall(name.to_string()));
            }
        }
        _ => return Err(Error::UnsupportedExpression(left.to_string())),
    }
    match *right {
        Expr::FunctionCall(name, args) => {
            if is_supported_function_call(&name) {
                if name == "write" {
                    eval_write(context, args)?;
                } else {
                    return Err(Error::UnsupportedFunctionCall(name.to_string()));
                }
            } else {
                return Err(Error::UnsupportedFunctionCall(name.to_string()));
            }
        }
        _ => return Err(Error::UnsupportedExpression(right.to_string())),
    }
    Ok(())
}

/// Returns true if the function call is supported.
fn is_supported_function_call(name: &str) -> bool {
    name == "read" || name == "write"
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
