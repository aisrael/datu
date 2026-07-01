//! Concatenates multiple input files into a single output file (`datu concat`).

use datafusion::prelude::DataFrame;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelinePlanningError;
use crate::pipeline::dataframe::DataFrameSource;
use crate::pipeline::dataframe::read_dataframe_from_path;
use crate::pipeline::dataframe::write_dataframe_to_path;
use crate::resolve_file_type;

/// Reads each of `input_paths` in order (format inferred per-path unless overridden by
/// `input_type_override`), concatenates them via [`DataFrame::union`], and writes the combined
/// result to `output_path` (format inferred unless overridden by `output_type_override`).
///
/// All inputs must share a compatible schema; mismatches surface as a DataFusion error during
/// the union. Reuses the same per-format output dispatch as `convert`
/// ([`write_dataframe_to_path`]), so any supported conversion output format is also a valid
/// `concat` target.
#[allow(clippy::too_many_arguments)] // Mirrors PipelineBuilder's write-pipeline knobs (see builder.rs).
pub async fn concat_files(
    input_paths: &[String],
    input_type_override: Option<FileType>,
    output_path: &str,
    output_type_override: Option<FileType>,
    csv_has_header: Option<bool>,
    sparse: bool,
    json_pretty: bool,
    progress: Option<ProgressBar>,
) -> Result<()> {
    if input_paths.is_empty() {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::MissingRequiredStage(
                "concat requires at least one input file".to_string(),
            ),
        ));
    }

    let output_file_type = resolve_file_type(output_type_override, output_path)?;
    if !output_file_type.supports_pipeline_conversion_output() {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedOutputFileType(output_file_type.to_string()),
        ));
    }

    let mut combined: Option<DataFrame> = None;
    for path in input_paths {
        let input_file_type = resolve_file_type(input_type_override, path)?;
        if !input_file_type.supports_pipeline_conversion_input() {
            return Err(Error::PipelinePlanningError(
                PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
            ));
        }
        let df = read_dataframe_from_path(path, input_file_type, csv_has_header).await?;
        combined = Some(match combined {
            Some(acc) => acc.union(df)?,
            None => df,
        });
    }

    let df = combined.expect("input_paths emptiness was checked above");
    let source = DataFrameSource::new(df);
    write_dataframe_to_path(
        source,
        output_path.to_string(),
        output_file_type,
        sparse,
        json_pretty,
        progress,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_requires_at_least_one_input() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.parquet");
        let result = concat_files(
            &[],
            None,
            output_path.to_str().expect("valid utf-8 path"),
            None,
            None,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_err(), "concat with no inputs should fail");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_single_avro_input() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.parquet");
        let result = concat_files(
            &["fixtures/concat_part1.avro".to_string()],
            None,
            output_path.to_str().expect("valid utf-8 path"),
            None,
            None,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_ok(), "concat failed: {:?}", result.err());
        assert!(output_path.exists(), "output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_multiple_avro_inputs() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("combined.parquet");
        let result = concat_files(
            &[
                "fixtures/concat_part1.avro".to_string(),
                "fixtures/concat_part2.avro".to_string(),
            ],
            None,
            output_path.to_str().expect("valid utf-8 path"),
            None,
            None,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_ok(), "concat failed: {:?}", result.err());
        assert!(output_path.exists(), "output file was not created");

        let total = crate::get_total_rows_result(
            output_path.to_str().expect("valid utf-8 path"),
            FileType::Parquet,
        )
        .expect("failed to read total rows");
        let count1 = crate::get_total_rows_result("fixtures/concat_part1.avro", FileType::Avro);
        let count2 = crate::get_total_rows_result("fixtures/concat_part2.avro", FileType::Avro);
        if let (Ok(c1), Ok(c2)) = (count1, count2) {
            assert_eq!(total, c1 + c2, "concatenated row count should be the sum");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_schema_mismatch_errors() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("combined.parquet");
        let result = concat_files(
            &[
                "fixtures/file1.avro".to_string(),
                "fixtures/file2.avro".to_string(),
            ],
            None,
            output_path.to_str().expect("valid utf-8 path"),
            None,
            None,
            true,
            false,
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "concat of files with incompatible schemas should fail"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_unsupported_output_type() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.png");
        let result = concat_files(
            &["fixtures/concat_part1.avro".to_string()],
            None,
            output_path.to_str().expect("valid utf-8 path"),
            None,
            None,
            true,
            false,
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "concat with unknown output type should fail"
        );
    }
}
