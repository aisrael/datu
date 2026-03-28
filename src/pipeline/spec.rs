//! Column selection types and display slice for pipeline head/tail/sample.

use arrow::datatypes::Schema;

use crate::Error;
use crate::Result;
use crate::errors::PipelinePlanningError;

/// Head, tail, or random sample of rows to send to stdout for a display pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplaySlice {
    Head(usize),
    Tail(usize),
    Sample(usize),
}

/// How to match a column name: exact (case-sensitive) or case-insensitive.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// Exact match (from string literal like "column").
    Exact(String),
    /// Case-insensitive match (from symbol like :column or bare identifier).
    CaseInsensitive(String),
}

/// Macro to build a [`SelectSpec`] of [`ColumnSpec::Exact`] entries from column names.
#[macro_export]
macro_rules! select_spec {
    ( $($col:expr),+ $(,)? ) => {
        $crate::pipeline::SelectSpec {
            columns: vec![
                $(
                    $crate::pipeline::ColumnSpec::Exact($col.to_string())
                ),+
            ],
        }
    };
}

impl ColumnSpec {
    /// Resolves this spec against a schema, returning the actual column name.
    pub fn resolve(&self, schema: &Schema) -> Result<String> {
        match self {
            ColumnSpec::Exact(name) => {
                schema.index_of(name)?;
                Ok(name.clone())
            }
            ColumnSpec::CaseInsensitive(name) => schema
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(name))
                .map(|f| f.name().clone())
                .ok_or_else(|| {
                    Error::PipelinePlanningError(PipelinePlanningError::ColumnNotFound(
                        name.clone(),
                    ))
                }),
        }
    }
}

/// Column selection: ordered list of [`ColumnSpec`] entries (e.g. from CLI or REPL).
#[derive(Clone, Debug, PartialEq)]
pub struct SelectSpec {
    pub columns: Vec<ColumnSpec>,
}

impl SelectSpec {
    /// Returns true when the selection has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Parses CLI `--select` values from `Option<Vec<String>>`: splits each string on commas,
    /// trims, drops empties, maps to [`ColumnSpec::Exact`]. Returns `None` when `select` is `None`
    /// or yields no column names.
    pub fn from_cli_args(select: &Option<Vec<String>>) -> Option<Self> {
        let inner = select.as_ref()?;
        let mut columns = Vec::new();
        for s in inner {
            columns.extend(s.split(',').filter_map(|c| {
                let c = c.trim();
                if c.is_empty() {
                    None
                } else {
                    Some(ColumnSpec::Exact(c.to_string()))
                }
            }));
        }
        if columns.is_empty() {
            None
        } else {
            Some(Self { columns })
        }
    }

    /// Resolves all column specs against `schema`, returning actual column names in order.
    pub fn resolve_names(&self, schema: &Schema) -> Result<Vec<String>> {
        crate::pipeline::select::resolve_column_specs(schema, &self.columns)
    }
}
