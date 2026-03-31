//! Maps validated REPL stages to [`crate::pipeline::PipelineBuilder`].

use super::stage::ReplPipelineStage;
use crate::pipeline::PipelineBuilder;
use crate::pipeline::SelectSpec;

/// Maps validated REPL stages to a [`PipelineBuilder`] (caller sets display defaults if needed).
pub(super) fn repl_stages_to_pipeline_builder(
    stages: &[ReplPipelineStage],
) -> crate::Result<PipelineBuilder> {
    let body = match stages.last() {
        Some(ReplPipelineStage::Print) if stages.len() >= 2 => &stages[..stages.len() - 1],
        _ => stages,
    };

    let path = match body.first() {
        Some(ReplPipelineStage::Read { path }) => path.as_str(),
        _ => {
            return Err(crate::Error::InvalidReplPipeline(
                "pipeline must start with read(path)".to_string(),
            ));
        }
    };

    let mut builder = PipelineBuilder::new();
    builder.read(path);

    let mut i = 1usize;
    if let Some(ReplPipelineStage::Select { columns }) = body.get(i) {
        builder.select_spec(SelectSpec {
            columns: columns.to_vec(),
        });
        i += 1;
    }

    match body.get(i) {
        Some(ReplPipelineStage::Head { n }) => {
            builder.head(*n);
            i += 1;
        }
        Some(ReplPipelineStage::Tail { n }) => {
            builder.tail(*n);
            i += 1;
        }
        Some(ReplPipelineStage::Sample { n }) => {
            builder.sample(*n);
            i += 1;
        }
        Some(ReplPipelineStage::Schema) => {
            builder.schema();
            i += 1;
        }
        Some(ReplPipelineStage::Count) => {
            builder.row_count();
            i += 1;
        }
        _ => {}
    }

    if let Some(ReplPipelineStage::Write { path }) = body.get(i) {
        builder.write(path);
    }

    Ok(builder)
}
