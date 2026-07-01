//! REPL pipeline stage AST (planned, not yet executed).

use std::fmt;

use crate::pipeline::ColumnSpec;
use crate::pipeline::GroupByKey;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;

/// A planned pipeline stage with validated, extracted arguments.
#[derive(Debug, PartialEq)]
pub enum ReplPipelineStage {
    Read {
        path: String,
    },
    /// SQL predicate (`parse_sql_expr` + `filter`); before or after the `select` stage per pipeline order (after includes post-aggregate when `group_by` is used).
    Filter {
        sql: String,
    },
    GroupBy {
        columns: Vec<GroupByKey>,
    },
    Select {
        columns: Vec<SelectItem>,
    },
    Head {
        n: usize,
    },
    Tail {
        n: usize,
    },
    Sample {
        n: usize,
    },
    Schema,
    Count,
    Write {
        path: String,
    },
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
            ReplPipelineStage::GroupBy { .. } | ReplPipelineStage::Filter { .. } => false,
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

/// Quotes an alias key when it isn't a valid bare identifier (mirrors
/// `flt::ast::expr::KeyValue`'s own `Display`, which applies the same rule to map literals).
fn format_alias_key(key: &str) -> String {
    let is_bare = key
        .chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_')
        && key.chars().all(|c| c.is_alphanumeric() || c == '_');
    if is_bare {
        key.to_string()
    } else {
        format!("{key:?}")
    }
}

fn format_group_by_key(key: &GroupByKey) -> String {
    let body = format_column_spec(&key.spec);
    match &key.alias {
        Some(alias) => format!("{}: {body}", format_alias_key(alias)),
        None => body,
    }
}

fn format_select_item(item: &SelectItem) -> String {
    let body = match item {
        SelectItem::Column(c, _) => format_column_spec(c),
        SelectItem::Sum(c, _) => format!("sum({})", format_column_spec(c)),
        SelectItem::Avg(c, _) => format!("avg({})", format_column_spec(c)),
        SelectItem::Min(c, _) => format!("min({})", format_column_spec(c)),
        SelectItem::Max(c, _) => format!("max({})", format_column_spec(c)),
        SelectItem::Count(c, _) => format!("count({})", format_column_spec(c)),
        SelectItem::CountDistinct(c, _) => format!("count_distinct({})", format_column_spec(c)),
    };
    match item.alias() {
        Some(alias) => format!("{}: {body}", format_alias_key(alias)),
        None => body,
    }
}

impl fmt::Display for ReplPipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplPipelineStage::Read { path } => write!(f, r#"read("{path}")"#),
            ReplPipelineStage::Filter { sql } => write!(f, "filter({sql:?})"),
            ReplPipelineStage::GroupBy { columns } => {
                let cols: Vec<String> = columns.iter().map(format_group_by_key).collect();
                write!(f, "group_by({})", cols.join(", "))
            }
            ReplPipelineStage::Select { columns } => {
                let cols: Vec<String> = columns.iter().map(format_select_item).collect();
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
