//! Interactive REPL: flt expressions, pipeline planning, and execution.

mod builder_bridge;
mod interactive;
mod plan;
mod stage;

pub use interactive::Repl;
pub use stage::ReplPipelineStage;

/// Column selection in REPL expressions (re-export of [`crate::pipeline::ColumnSpec`]).
pub use crate::pipeline::ColumnSpec;
pub use crate::pipeline::SelectItem;

#[cfg(test)]
mod tests;
