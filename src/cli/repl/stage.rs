//! REPL pipeline stage AST (planned, not yet executed).

use std::fmt;

use crate::pipeline::ColumnSpec;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
pub enum ReplPipelineStage {
    Read { path: String },
    Select { columns: Vec<ColumnSpec> },
    Head { n: usize },
    Tail { n: usize },
    Sample { n: usize },
    Schema,
    Count,
    Write { path: String },
    Print,
}

impl ReplPipelineStage {
    /// Returns true when a stage closes a REPL statement.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReplPipelineStage::Head { .. }
                | ReplPipelineStage::Tail { .. }
                | ReplPipelineStage::Sample { .. }
                | ReplPipelineStage::Schema
                | ReplPipelineStage::Count
                | ReplPipelineStage::Write { .. }
        )
    }

    /// Returns true for stages that can continue to another explicit stage.
    pub fn is_non_terminal(&self) -> bool {
        !self.is_terminal()
    }

    /// Returns any implicit stage that should be appended after this stage.
    pub fn get_implicit_followup_stage(&self) -> Option<ReplPipelineStage> {
        match self {
            ReplPipelineStage::Head { .. }
            | ReplPipelineStage::Tail { .. }
            | ReplPipelineStage::Sample { .. } => Some(ReplPipelineStage::Print),
            _ => None,
        }
    }
}

impl fmt::Display for ReplPipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplPipelineStage::Read { path } => write!(f, r#"read("{path}")"#),
            ReplPipelineStage::Select { columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| match c {
                        ColumnSpec::Exact(s) => format!(r#""{s}""#),
                        ColumnSpec::CaseInsensitive(s) => format!(":{s}"),
                    })
                    .collect::<Vec<_>>();
                write!(f, "select({})", cols.join(", "))
            }
            ReplPipelineStage::Head { n } => write!(f, "head({n})"),
            ReplPipelineStage::Tail { n } => write!(f, "tail({n})"),
            ReplPipelineStage::Sample { n } => write!(f, "sample({n})"),
            ReplPipelineStage::Schema => write!(f, "schema()"),
            ReplPipelineStage::Count => write!(f, "count()"),
            ReplPipelineStage::Write { path } => write!(f, r#"write("{path}")"#),
            ReplPipelineStage::Print => write!(f, "print()"),
        }
    }
}
