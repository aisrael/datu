//! Maps validated REPL stages to [`crate::pipeline::PipelineBuilder`].

use super::stage::ReplPipelineStage;
use crate::pipeline::ColumnSpec;
use crate::pipeline::PipelineBuilder;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;

/// Maps validated REPL stages to a [`PipelineBuilder`] (caller sets display defaults if needed).
pub(crate) fn repl_stages_to_pipeline_builder(
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
    let mut filter_idx: Option<usize> = None;
    let mut select_idx: Option<usize> = None;
    let mut group_keys: Option<Vec<ColumnSpec>> = None;
    let mut select_columns: Option<Vec<SelectItem>> = None;
    let mut filter_sql: Option<String> = None;

    while i < body.len() {
        match body.get(i) {
            Some(ReplPipelineStage::Filter { sql }) => {
                filter_idx = Some(i);
                filter_sql = Some(sql.clone());
                i += 1;
            }
            Some(ReplPipelineStage::GroupBy { columns }) => {
                group_keys = Some(columns.clone());
                i += 1;
            }
            Some(ReplPipelineStage::Select { columns }) => {
                select_idx = Some(i);
                select_columns = Some(columns.clone());
                i += 1;
            }
            Some(
                ReplPipelineStage::Head { .. }
                | ReplPipelineStage::Tail { .. }
                | ReplPipelineStage::Sample { .. }
                | ReplPipelineStage::Schema
                | ReplPipelineStage::Count
                | ReplPipelineStage::Write { .. },
            ) => break,
            Some(ReplPipelineStage::Read { .. }) => {
                return Err(crate::Error::InvalidReplPipeline(
                    "unexpected read(path) after start of pipeline".to_string(),
                ));
            }
            Some(ReplPipelineStage::Print) => {
                return Err(crate::Error::InvalidReplPipeline(
                    "unexpected print() in pipeline body".to_string(),
                ));
            }
            None => break,
        }
    }

    if let Some(columns) = select_columns {
        let spec = SelectSpec {
            columns,
            group_by: group_keys,
        };
        builder.select_spec(spec);
    }

    if let Some(sql) = filter_sql {
        let runs_after = match (filter_idx, select_idx) {
            (Some(f), Some(s)) => f > s,
            _ => false,
        };
        builder.filter_sql(&sql);
        builder.filter_runs_after_select(runs_after);
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
