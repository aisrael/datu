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

/// One entry in a `select()`: plain column or aggregate.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    /// Project a column (CLI `--select`, REPL symbols/strings).
    Column(ColumnSpec),
    /// Global sum over one column (REPL `sum(:col)`).
    Sum(ColumnSpec),
}

/// Macro to build a [`SelectSpec`] from homogeneous column forms:
/// - string literals (`"col"`) -> [`ColumnSpec::Exact`]
/// - symbol-like idents (`:col`) -> [`ColumnSpec::CaseInsensitive`]
#[macro_export]
macro_rules! select_spec {
    ( $($col:literal),+ $(,)? ) => {
        $crate::pipeline::SelectSpec {
            columns: vec![
                $(
                    $crate::pipeline::SelectItem::Column(
                        $crate::pipeline::ColumnSpec::Exact($col.to_string())
                    )
                ),+
            ],
            group_by: None,
        }
    };
    ( $(:$col:ident),+ $(,)? ) => {
        $crate::pipeline::SelectSpec {
            columns: vec![
                $(
                    $crate::pipeline::SelectItem::Column(
                        $crate::pipeline::ColumnSpec::CaseInsensitive(stringify!($col).to_string())
                    )
                ),+
            ],
            group_by: None,
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

impl SelectItem {
    /// Returns true when this item is an aggregate (not a plain projection).
    pub fn is_aggregate(&self) -> bool {
        matches!(self, SelectItem::Sum(_))
    }
}

/// Column selection: ordered list of items (projections and/or aggregates).
#[derive(Clone, Debug, PartialEq)]
pub struct SelectSpec {
    pub columns: Vec<SelectItem>,
    /// REPL `group_by(...)` keys; `None` for CLI and plain projection.
    pub group_by: Option<Vec<ColumnSpec>>,
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

    /// True if any entry is an aggregate (`sum`, etc.).
    pub fn has_aggregates(&self) -> bool {
        self.columns.iter().any(SelectItem::is_aggregate)
    }

    /// True when every entry is an aggregate (global aggregation, no plain columns).
    pub fn is_aggregate_only(&self) -> bool {
        !self.columns.is_empty() && self.columns.iter().all(SelectItem::is_aggregate)
    }

    /// True when the REPL attached `group_by(...)`.
    pub fn has_group_by(&self) -> bool {
        self.group_by.as_ref().is_some_and(|g| !g.is_empty())
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
                    Some(SelectItem::Column(ColumnSpec::Exact(c.to_string())))
                }
            }));
        }
        if columns.is_empty() {
            None
        } else {
            Some(Self {
                columns,
                group_by: None,
            })
        }
    }

    /// Resolves plain column specs against `schema`, returning actual column names in order.
    /// Fails if the spec contains aggregates (use the DataFrame aggregate path instead).
    pub fn resolve_names(&self, schema: &Schema) -> Result<Vec<String>> {
        self.columns
            .iter()
            .map(|item| match item {
                SelectItem::Column(s) => s.resolve(schema),
                SelectItem::Sum(_) => Err(Error::PipelinePlanningError(
                    PipelinePlanningError::AggregatesInProjectionSelect,
                )),
            })
            .collect()
    }
}

/// Indexes into a [`SelectSpec`] by column index.
impl Index<usize> for SelectSpec {
    type Output = SelectItem;

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
    use super::SelectItem;

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
        let spec = crate::select_spec!("one", "three");
        let resolved = spec.resolve_names(&schema).unwrap();
        assert_eq!(resolved, vec!["one", "three"]);
    }

    #[test]
    fn test_select_spec_resolve_exact_no_match_wrong_case() {
        let schema = schema_with_columns(&["one", "two"]);
        let spec = crate::select_spec!("ONE");
        let result = spec.resolve_names(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_spec_resolve_case_insensitive_match() {
        let schema = schema_with_columns(&["One", "two", "Email"]);
        let spec = crate::select_spec!(:ONE, :email);
        let resolved = spec.resolve_names(&schema).unwrap();
        assert_eq!(resolved, vec!["One", "Email"]);
    }

    #[test]
    fn test_select_spec_resolve_case_insensitive_no_match() {
        let schema = schema_with_columns(&["one", "two"]);
        let spec = crate::select_spec!(:missing);
        let result = spec.resolve_names(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_spec_macro_strings_create_exact_columns() {
        let spec = crate::select_spec!("one", "two");
        assert_eq!(
            spec.columns,
            vec![
                SelectItem::Column(ColumnSpec::Exact("one".into())),
                SelectItem::Column(ColumnSpec::Exact("two".into())),
            ]
        );
        assert_eq!(spec.group_by, None);
    }

    #[test]
    fn test_select_spec_macro_symbols_create_case_insensitive_columns() {
        let spec = crate::select_spec!(:one, :two);
        assert_eq!(
            spec.columns,
            vec![
                SelectItem::Column(ColumnSpec::CaseInsensitive("one".into())),
                SelectItem::Column(ColumnSpec::CaseInsensitive("two".into())),
            ]
        );
        assert_eq!(spec.group_by, None);
    }
}
