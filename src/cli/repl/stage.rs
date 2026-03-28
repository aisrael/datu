//! REPL pipeline stage AST (planned, not yet executed).

use std::fmt;

use crate::pipeline::ColumnSpec;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
pub enum PipelineStage {
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

impl PipelineStage {
    /// Returns true when a stage closes a REPL statement.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipelineStage::Head { .. }
                | PipelineStage::Tail { .. }
                | PipelineStage::Sample { .. }
                | PipelineStage::Schema
                | PipelineStage::Count
                | PipelineStage::Write { .. }
        )
    }

    /// Returns true for stages that can continue to another explicit stage.
    pub fn is_non_terminal(&self) -> bool {
        !self.is_terminal()
    }

    /// Returns any implicit stage that should be appended after this stage.
    pub fn implicit_followup_stage(&self) -> Option<PipelineStage> {
        match self {
            PipelineStage::Head { .. }
            | PipelineStage::Tail { .. }
            | PipelineStage::Sample { .. } => Some(PipelineStage::Print),
            _ => None,
        }
    }
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineStage::Read { path } => write!(f, r#"read("{path}")"#),
            PipelineStage::Select { columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| match c {
                        ColumnSpec::Exact(s) => format!(r#""{s}""#),
                        ColumnSpec::CaseInsensitive(s) => format!(":{s}"),
                    })
                    .collect::<Vec<_>>();
                write!(f, "select({})", cols.join(", "))
            }
            PipelineStage::Head { n } => write!(f, "head({n})"),
            PipelineStage::Tail { n } => write!(f, "tail({n})"),
            PipelineStage::Sample { n } => write!(f, "sample({n})"),
            PipelineStage::Schema => write!(f, "schema()"),
            PipelineStage::Count => write!(f, "count()"),
            PipelineStage::Write { path } => write!(f, r#"write("{path}")"#),
            PipelineStage::Print => write!(f, "print()"),
        }
    }
}
