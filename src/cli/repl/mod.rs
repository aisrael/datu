//! Interactive REPL: flt expressions, pipeline planning, and execution.

mod builder_bridge;
mod interactive;
mod pipeline;
mod plan;
mod stage;

pub use interactive::Repl;
pub use pipeline::ReplPipeline;
pub use stage::PipelineStage;

/// Column selection in REPL expressions (re-export of [`crate::pipeline::ColumnSpec`]).
pub use crate::pipeline::ColumnSpec;

#[cfg(test)]
mod tests;
