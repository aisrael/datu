//! REPL for datu

use anyhow::Result;
use datu::Error;
use flt::ast::BinaryOp;
use flt::ast::Expr;
use flt::parser::parse_expr;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

/// Maximum number of inputs to keep in REPL history.
const HISTORY_DEPTH: usize = 1000;

/// Context for a REPL session.
pub struct ReplContext {}

/// Runs the datu REPL.
pub fn run() -> Result<()> {
    let config = rustyline::Config::builder()
        .max_history_size(HISTORY_DEPTH)?
        .auto_add_history(true)
        .build();
    let mut rl = DefaultEditor::with_config(config)?;
    let mut context = ReplContext {};
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
                println!("name: {name:?}");
                println!("args: {args:?}");
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
fn eval_read(_context: &mut ReplContext, args: Vec<Expr>) -> datu::Result<()> {
    println!("args: {args:?}");
    Ok(())
}

/// Evaluates a write function call.
fn eval_write(_context: &mut ReplContext, args: Vec<Expr>) -> datu::Result<()> {
    println!("args: {args:?}");
    Ok(())
}
