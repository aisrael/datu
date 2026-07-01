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

/// SQL predicate string for DataFusion `parse_sql_expr` + `filter` (newtype over `String`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterSpec(String);

impl FilterSpec {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for FilterSpec {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for FilterSpec {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// How to match a column name: exact (case-sensitive) or case-insensitive.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// Exact match (from string literal like "column").
    Exact(String),
    /// Case-insensitive match (from symbol like :column or bare identifier).
    CaseInsensitive(String),
}

impl ColumnSpec {
    /// Returns the raw name text, regardless of match kind (exact vs. case-insensitive).
    pub fn name(&self) -> &str {
        match self {
            ColumnSpec::Exact(s) | ColumnSpec::CaseInsensitive(s) => s,
        }
    }
}

/// One `group_by(...)` key, with an optional output alias (REPL `name: value` /
/// `"quoted name": value` keyword syntax). The alias is the *default* output name for this
/// key's column in `select()`; an alias on the matching `SelectItem::Column` in `select()`
/// takes precedence when present.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupByKey {
    pub spec: ColumnSpec,
    pub alias: Option<String>,
}

impl GroupByKey {
    /// Constructs a group-by key with no alias.
    pub fn new(spec: ColumnSpec) -> Self {
        Self { spec, alias: None }
    }

    /// Returns a copy of this key with its alias replaced.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// True when `c` refers to this group key in `select()`: either the key's underlying
    /// column, or (when present) this key's `group_by()` alias, compared case-insensitively
    /// since an alias is a label rather than a physical schema column.
    pub fn matches_select_column(&self, c: &ColumnSpec) -> bool {
        &self.spec == c
            || self
                .alias
                .as_deref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(c.name()))
    }
}

impl PartialEq<ColumnSpec> for GroupByKey {
    fn eq(&self, other: &ColumnSpec) -> bool {
        &self.spec == other
    }
}

/// One entry in a `select()`: plain column or aggregate, with an optional output alias
/// (REPL `name: value` / `"quoted name": value` keyword syntax).
#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    /// Project a column (CLI `--select`, REPL symbols/strings).
    Column(ColumnSpec, Option<String>),
    /// Global sum over one column (REPL `sum(:col)`).
    Sum(ColumnSpec, Option<String>),
    /// Global average over one column (REPL `avg(:col)`).
    Avg(ColumnSpec, Option<String>),
    /// Global minimum over one column (REPL `min(:col)`).
    Min(ColumnSpec, Option<String>),
    /// Global maximum over one column (REPL `max(:col)`).
    Max(ColumnSpec, Option<String>),
    /// Count of non-null values in one column (REPL `count(:col)`).
    Count(ColumnSpec, Option<String>),
    /// Count of distinct non-null values in one column (REPL `count_distinct(:col)`).
    CountDistinct(ColumnSpec, Option<String>),
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
                    $crate::pipeline::SelectItem::column(
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
                    $crate::pipeline::SelectItem::column(
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
    /// Constructs a plain column projection with no alias.
    pub fn column(spec: ColumnSpec) -> Self {
        Self::Column(spec, None)
    }

    /// Constructs a `sum(...)` aggregate with no alias.
    pub fn sum(spec: ColumnSpec) -> Self {
        Self::Sum(spec, None)
    }

    /// Constructs an `avg(...)` aggregate with no alias.
    pub fn avg(spec: ColumnSpec) -> Self {
        Self::Avg(spec, None)
    }

    /// Constructs a `min(...)` aggregate with no alias.
    pub fn min(spec: ColumnSpec) -> Self {
        Self::Min(spec, None)
    }

    /// Constructs a `max(...)` aggregate with no alias.
    pub fn max(spec: ColumnSpec) -> Self {
        Self::Max(spec, None)
    }

    /// Constructs a `count(...)` aggregate with no alias.
    pub fn count(spec: ColumnSpec) -> Self {
        Self::Count(spec, None)
    }

    /// Constructs a `count_distinct(...)` aggregate with no alias.
    pub fn count_distinct(spec: ColumnSpec) -> Self {
        Self::CountDistinct(spec, None)
    }

    /// Returns a copy of this item with its alias replaced (REPL `name: value` syntax).
    pub fn with_alias(self, alias: impl Into<String>) -> Self {
        let alias = Some(alias.into());
        match self {
            Self::Column(s, _) => Self::Column(s, alias),
            Self::Sum(s, _) => Self::Sum(s, alias),
            Self::Avg(s, _) => Self::Avg(s, alias),
            Self::Min(s, _) => Self::Min(s, alias),
            Self::Max(s, _) => Self::Max(s, alias),
            Self::Count(s, _) => Self::Count(s, alias),
            Self::CountDistinct(s, _) => Self::CountDistinct(s, alias),
        }
    }

    /// Returns the output alias, if one was given (REPL `name: value` syntax).
    pub fn alias(&self) -> Option<&str> {
        match self {
            Self::Column(_, a)
            | Self::Sum(_, a)
            | Self::Avg(_, a)
            | Self::Min(_, a)
            | Self::Max(_, a)
            | Self::Count(_, a)
            | Self::CountDistinct(_, a) => a.as_deref(),
        }
    }

    /// Returns the underlying column spec, regardless of aggregate kind or alias.
    pub fn column_spec(&self) -> &ColumnSpec {
        match self {
            Self::Column(s, _)
            | Self::Sum(s, _)
            | Self::Avg(s, _)
            | Self::Min(s, _)
            | Self::Max(s, _)
            | Self::Count(s, _)
            | Self::CountDistinct(s, _) => s,
        }
    }

    /// Returns true when this item is an aggregate (not a plain projection).
    pub fn is_aggregate(&self) -> bool {
        matches!(
            self,
            SelectItem::Sum(..)
                | SelectItem::Avg(..)
                | SelectItem::Min(..)
                | SelectItem::Max(..)
                | SelectItem::Count(..)
                | SelectItem::CountDistinct(..)
        )
    }
}

/// Column selection: ordered list of items (projections and/or aggregates).
#[derive(Clone, Debug, PartialEq)]
pub struct SelectSpec {
    pub columns: Vec<SelectItem>,
    /// REPL `group_by(...)` keys; `None` for CLI and plain projection.
    pub group_by: Option<Vec<GroupByKey>>,
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
                    Some(SelectItem::column(ColumnSpec::Exact(c.to_string())))
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
                SelectItem::Column(s, _) => s.resolve(schema),
                SelectItem::Sum(..)
                | SelectItem::Avg(..)
                | SelectItem::Min(..)
                | SelectItem::Max(..)
                | SelectItem::Count(..)
                | SelectItem::CountDistinct(..) => Err(Error::PipelinePlanningError(
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
    use super::GroupByKey;
    use super::SelectItem;

    #[test]
    fn test_group_by_key_no_alias_by_default() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into()));
        assert_eq!(key.alias, None);
        assert_eq!(key.spec, ColumnSpec::CaseInsensitive("bar".into()));
    }

    #[test]
    fn test_group_by_key_with_alias_sets_alias() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar");
        assert_eq!(key.alias.as_deref(), Some("foo_bar"));
    }

    #[test]
    fn test_group_by_key_eq_column_spec_ignores_alias() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar");
        assert_eq!(key, ColumnSpec::CaseInsensitive("bar".into()));
        assert_ne!(key, ColumnSpec::CaseInsensitive("baz".into()));
    }

    #[test]
    fn test_group_by_key_matches_select_column_by_underlying_spec() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()));
        assert!(key.matches_select_column(&ColumnSpec::CaseInsensitive("id".into())));
        assert!(!key.matches_select_column(&ColumnSpec::CaseInsensitive("other".into())));
    }

    #[test]
    fn test_group_by_key_matches_select_column_by_alias() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("id".into())).with_alias("key");
        assert!(key.matches_select_column(&ColumnSpec::CaseInsensitive("key".into())));
        // Underlying column still matches too.
        assert!(key.matches_select_column(&ColumnSpec::CaseInsensitive("id".into())));
        assert!(!key.matches_select_column(&ColumnSpec::CaseInsensitive("other".into())));
    }

    #[test]
    fn test_group_by_key_matches_select_column_by_alias_is_case_insensitive() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("id".into())).with_alias("key");
        assert!(key.matches_select_column(&ColumnSpec::Exact("KEY".into())));
    }

    #[test]
    fn test_group_by_key_without_alias_does_not_match_arbitrary_name() {
        let key = GroupByKey::new(ColumnSpec::CaseInsensitive("id".into()));
        assert!(!key.matches_select_column(&ColumnSpec::CaseInsensitive("key".into())));
    }

    #[test]
    fn test_select_item_avg_is_aggregate() {
        let item = SelectItem::avg(ColumnSpec::CaseInsensitive("x".into()));
        assert!(item.is_aggregate());
    }

    #[test]
    fn test_select_item_min_max_are_aggregate() {
        let min_item = SelectItem::min(ColumnSpec::CaseInsensitive("x".into()));
        let max_item = SelectItem::max(ColumnSpec::CaseInsensitive("y".into()));
        assert!(min_item.is_aggregate());
        assert!(max_item.is_aggregate());
    }

    #[test]
    fn test_select_item_count_aggregates_are_aggregate() {
        let c = ColumnSpec::CaseInsensitive("x".into());
        assert!(SelectItem::count(c.clone()).is_aggregate());
        assert!(SelectItem::count_distinct(c).is_aggregate());
    }

    #[test]
    fn test_select_item_with_alias_sets_alias() {
        let item =
            SelectItem::column(ColumnSpec::CaseInsensitive("bar".into())).with_alias("foo_bar");
        assert_eq!(item.alias(), Some("foo_bar"));
        assert_eq!(
            item.column_spec(),
            &ColumnSpec::CaseInsensitive("bar".into())
        );
    }

    #[test]
    fn test_select_item_no_alias_by_default() {
        let item = SelectItem::sum(ColumnSpec::CaseInsensitive("qty".into()));
        assert_eq!(item.alias(), None);
    }

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
                SelectItem::column(ColumnSpec::Exact("one".into())),
                SelectItem::column(ColumnSpec::Exact("two".into())),
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
                SelectItem::column(ColumnSpec::CaseInsensitive("one".into())),
                SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
            ]
        );
        assert_eq!(spec.group_by, None);
    }
}
