//! REPL pipeline stage AST (planned, not yet executed).

use std::fmt;

use crate::pipeline::ColumnSpec;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
pub enum ReplPipelineStage {
    Read { path: String },
    GroupBy { columns: Vec<ColumnSpec> },
    Select { columns: Vec<SelectItem> },
    Head { n: usize },
    Tail { n: usize },
    Sample { n: usize },
    Schema,
    Count,
    Write { path: String },
    Print,
}

fn select_spec_from_items(columns: &[SelectItem]) -> SelectSpec {
    SelectSpec {
        columns: columns.to_vec(),
        group_by: None,
    }
}

/// True when grouped + `select` stages form a complete statement (including `select(...) |> group_by(...)`).
pub(super) fn repl_pipeline_last_select_is_terminal(stages: &[ReplPipelineStage]) -> bool {
    let has_group_by = stages
        .iter()
        .any(|s| matches!(s, ReplPipelineStage::GroupBy { .. }));
    let has_select = stages
        .iter()
        .any(|s| matches!(s, ReplPipelineStage::Select { .. }));

    if let Some(ReplPipelineStage::Select { columns }) = stages.last() {
        let spec = SelectSpec {
            columns: columns.clone(),
            group_by: None,
        };
        if spec.is_aggregate_only() {
            return true;
        }
        if has_group_by {
            return true;
        }
        return false;
    }

    matches!(stages.last(), Some(ReplPipelineStage::GroupBy { .. })) && has_select
}

impl ReplPipelineStage {
    /// Returns true when a stage closes a REPL statement.
    pub fn is_terminal(&self) -> bool {
        match self {
            ReplPipelineStage::Head { .. }
            | ReplPipelineStage::Tail { .. }
            | ReplPipelineStage::Sample { .. }
            | ReplPipelineStage::Schema
            | ReplPipelineStage::Count
            | ReplPipelineStage::Write { .. } => true,
            ReplPipelineStage::Select { columns } => {
                // Single-stage check: full pipeline uses `repl_pipeline_last_select_is_terminal`.
                select_spec_from_items(columns).is_aggregate_only()
            }
            ReplPipelineStage::GroupBy { .. } => false,
            ReplPipelineStage::Read { .. } | ReplPipelineStage::Print => false,
        }
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

fn format_column_spec(c: &ColumnSpec) -> String {
    match c {
        ColumnSpec::Exact(s) => format!(r#""{s}""#),
        ColumnSpec::CaseInsensitive(s) => format!(":{s}"),
    }
}

impl fmt::Display for ReplPipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplPipelineStage::Read { path } => write!(f, r#"read("{path}")"#),
            ReplPipelineStage::GroupBy { columns } => {
                let cols: Vec<String> = columns.iter().map(format_column_spec).collect();
                write!(f, "group_by({})", cols.join(", "))
            }
            ReplPipelineStage::Select { columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|item| match item {
                        SelectItem::Column(c) => format_column_spec(c),
                        SelectItem::Sum(c) => format!("sum({})", format_column_spec(c)),
                        SelectItem::Avg(c) => format!("avg({})", format_column_spec(c)),
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
