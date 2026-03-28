//! Column selection types and display slice for pipeline head/tail/sample.

use std::ops::Index;

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

    /// Returns the number of columns in the selection.
    pub fn len(&self) -> usize {
        self.columns.len()
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
        self.columns.iter().map(|s| s.resolve(schema)).collect()
    }
}

/// Indexes into a [`SelectSpec`] by column index.
impl Index<usize> for SelectSpec {
    type Output = ColumnSpec;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;

    use super::ColumnSpec;
    use super::SelectSpec;

    fn schema_with_columns(names: &[&str]) -> Schema {
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect();
        Schema::new(fields)
    }

    #[test]
    fn test_select_spec_resolve_exact_match() {
        let schema = schema_with_columns(&["one", "two", "three"]);
        let spec = SelectSpec {
            columns: vec![
                ColumnSpec::Exact("one".into()),
                ColumnSpec::Exact("three".into()),
            ],
        };
        let resolved = spec.resolve_names(&schema).unwrap();
        assert_eq!(resolved, vec!["one", "three"]);
    }

    #[test]
    fn test_select_spec_resolve_exact_no_match_wrong_case() {
        let schema = schema_with_columns(&["one", "two"]);
        let spec = SelectSpec {
            columns: vec![ColumnSpec::Exact("ONE".into())],
        };
        let result = spec.resolve_names(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_spec_resolve_case_insensitive_match() {
        let schema = schema_with_columns(&["one", "two", "Email"]);
        let spec = SelectSpec {
            columns: vec![
                ColumnSpec::CaseInsensitive("ONE".into()),
                ColumnSpec::CaseInsensitive("email".into()),
            ],
        };
        let resolved = spec.resolve_names(&schema).unwrap();
        assert_eq!(resolved, vec!["one", "Email"]);
    }

    #[test]
    fn test_select_spec_resolve_case_insensitive_no_match() {
        let schema = schema_with_columns(&["one", "two"]);
        let spec = SelectSpec {
            columns: vec![ColumnSpec::CaseInsensitive("missing".into())],
        };
        let result = spec.resolve_names(&schema);
        assert!(result.is_err());
    }
}
