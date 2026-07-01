//! Splits a single input file into multiple output files of at most N rows each (`datu split`).

use std::path::Path;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use indicatif::ProgressBar;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelinePlanningError;
use crate::pipeline::batch_readers::VecRecordBatchReader;
use crate::pipeline::batch_readers::build_reader;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::read::ReadResult;
use crate::pipeline::read::read_to_dataframe;
use crate::pipeline::step::Producer;
use crate::pipeline::write::write_record_batches_from_reader;
use crate::resolve_file_type;

/// Outcome of a successful [`split_file`] call.
pub struct SplitOutcome {
    pub partitions_written: usize,
    pub rows_written: usize,
}

/// Opens a lazy, row-by-row reader for `path`. JSON is the exception: DataFusion collects all
/// batches internally, so the result is materialized into a [`VecRecordBatchReader`] to give a
/// uniform iterator interface (mirrors `diff`'s `open_reader`).
async fn open_row_reader(
    path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<Box<dyn RecordBatchReader + 'static>> {
    match file_type {
        FileType::Json => {
            let result = read_to_dataframe(path, FileType::Json, csv_has_header).await?;
            let ReadResult::DataFrame(mut source) = result else {
                unreachable!("read_to_dataframe always returns ReadResult::DataFrame")
            };
            let df = source.get().await?;
            let batches = df.collect().await?;
            Ok(Box::new(VecRecordBatchReader::new(batches)))
        }
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc => {
            let mut read_args = ReadArgs::new(path, file_type);
            read_args.csv_has_header = csv_has_header;
            let mut reader_source = build_reader(&read_args)?;
            reader_source.get().await
        }
        _ => Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedInputFileType(file_type.to_string()),
        )),
    }
}

/// Streams a bounded number of rows per partition from a shared underlying reader. When a source
/// batch spans a partition boundary, the leftover slice is buffered in `pending` and consumed by
/// the *next* partition instead of being dropped.
struct PartitionReader<'a> {
    inner: &'a mut dyn RecordBatchReader,
    schema: SchemaRef,
    pending: Option<RecordBatch>,
    remaining: usize,
    consumed: usize,
}

impl<'a> PartitionReader<'a> {
    fn new(inner: &'a mut dyn RecordBatchReader) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            pending: None,
            remaining: 0,
            consumed: 0,
        }
    }

    /// Resets the row budget and consumed counter for the next partition.
    fn start_partition(&mut self, size: usize) {
        self.remaining = size;
        self.consumed = 0;
    }

    /// Ensures `pending` holds the next non-empty batch (if any) without spending any of the
    /// current partition's row budget, so callers can detect "no more data" before creating a
    /// trailing empty output file.
    fn ensure_pending(&mut self) -> Result<()> {
        while self.pending.is_none() {
            match self.inner.next() {
                None => break,
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                Some(Ok(b)) => self.pending = Some(b),
                Some(Err(e)) => return Err(Error::ArrowError(e)),
            }
        }
        Ok(())
    }

    fn has_more(&mut self) -> Result<bool> {
        self.ensure_pending()?;
        Ok(self.pending.is_some())
    }
}

impl RecordBatchReader for PartitionReader<'_> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for PartitionReader<'_> {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        loop {
            let batch = match self.pending.take() {
                Some(b) => b,
                None => match self.inner.next()? {
                    Ok(b) => b,
                    Err(e) => return Some(Err(e)),
                },
            };
            let rows = batch.num_rows();
            if rows == 0 {
                continue;
            }
            if rows <= self.remaining {
                self.remaining -= rows;
                self.consumed += rows;
                return Some(Ok(batch));
            }
            let take = self.remaining;
            self.pending = Some(batch.slice(take, rows - take));
            self.consumed += take;
            self.remaining = 0;
            return Some(Ok(batch.slice(0, take)));
        }
    }
}

/// Builds the output path for partition `index` (1-based) from `output_base`'s directory/stem and
/// `output_file_type`'s canonical extension, e.g. `dir/data.part00003.csv`.
fn partition_output_path(output_base: &str, output_file_type: FileType, index: usize) -> String {
    let base = Path::new(output_base);
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let file_name = format!("{stem}.part{index:05}.{output_file_type}");
    match base.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => parent.join(file_name).to_string_lossy().into_owned(),
        None => file_name,
    }
}

/// Reads `input_path` once and writes consecutive chunks of at most `split_size` rows to numbered
/// partition files derived from `output_base` (see [`partition_output_path`]). `limit` caps the
/// total number of rows processed across all partitions (`0` means unlimited).
#[allow(clippy::too_many_arguments)]
pub async fn split_file(
    input_path: &str,
    input_type_override: Option<FileType>,
    output_base: &str,
    output_type_override: Option<FileType>,
    csv_has_header: Option<bool>,
    split_size: usize,
    limit: usize,
    sparse: bool,
    json_pretty: bool,
    progress: Option<ProgressBar>,
) -> Result<SplitOutcome> {
    if split_size == 0 {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::MissingRequiredStage(
                "--split must be greater than 0".to_string(),
            ),
        ));
    }

    let input_file_type = resolve_file_type(input_type_override, input_path)?;
    if !input_file_type.supports_pipeline_conversion_input() {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedInputFileType(input_file_type.to_string()),
        ));
    }
    let output_file_type = resolve_file_type(output_type_override, output_base)?;
    if !output_file_type.supports_pipeline_conversion_output() {
        return Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedOutputFileType(output_file_type.to_string()),
        ));
    }

    let mut reader = open_row_reader(input_path, input_file_type, csv_has_header).await?;
    let mut partition_reader = PartitionReader::new(&mut *reader);

    let mut remaining_limit = (limit > 0).then_some(limit);
    let mut partition_index: usize = 1;
    let mut rows_written: usize = 0;

    loop {
        if remaining_limit == Some(0) {
            break;
        }
        if !partition_reader.has_more()? {
            break;
        }
        let this_size = match remaining_limit {
            Some(remaining) => split_size.min(remaining),
            None => split_size,
        };
        partition_reader.start_partition(this_size);
        let path = partition_output_path(output_base, output_file_type, partition_index);
        write_record_batches_from_reader(
            &mut partition_reader,
            &path,
            output_file_type,
            sparse,
            json_pretty,
        )?;

        let consumed = partition_reader.consumed;
        rows_written += consumed;
        if let Some(remaining) = remaining_limit.as_mut() {
            *remaining -= consumed;
        }
        if let Some(pb) = &progress {
            pb.set_message(format!(
                "Splitting {input_path}... ({partition_index} files, {rows_written} rows)"
            ));
        }
        partition_index += 1;
    }

    Ok(SplitOutcome {
        partitions_written: partition_index - 1,
        rows_written,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_output_path_with_parent() {
        let path = partition_output_path("dir/data.parquet", FileType::Parquet, 3);
        assert_eq!(path, "dir/data.part00003.parquet");
    }

    #[test]
    fn test_partition_output_path_without_parent() {
        let path = partition_output_path("data.avro", FileType::Avro, 1);
        assert_eq!(path, "data.part00001.avro");
    }

    #[test]
    fn test_partition_output_path_output_type_override_changes_extension() {
        let path = partition_output_path("data.avro", FileType::Csv, 12);
        assert_eq!(path, "data.part00012.csv");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_rejects_zero_split_size() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("out.parquet");
        let result = split_file(
            "fixtures/table.parquet",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            0,
            0,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_err(), "split with split_size 0 should fail");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_parquet_into_partitions() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("out.parquet");
        let outcome = split_file(
            "fixtures/table.parquet",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            2,
            0,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert_eq!(outcome.partitions_written, 2);
        assert_eq!(outcome.rows_written, 3);

        let part1 = temp_dir.path().join("out.part00001.parquet");
        let part2 = temp_dir.path().join("out.part00002.parquet");
        assert!(part1.exists());
        assert!(part2.exists());
        assert_eq!(
            crate::get_total_rows_result(
                part1.to_str().expect("valid utf-8 path"),
                FileType::Parquet
            )
            .expect("failed to read row count"),
            2
        );
        assert_eq!(
            crate::get_total_rows_result(
                part2.to_str().expect("valid utf-8 path"),
                FileType::Parquet
            )
            .expect("failed to read row count"),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_respects_limit() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("u.avro");
        let outcome = split_file(
            "fixtures/userdata5.avro",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            100,
            250,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert_eq!(outcome.partitions_written, 3);
        assert_eq!(outcome.rows_written, 250);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_default_naming_next_to_input() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let input_copy = temp_dir.path().join("table.parquet");
        std::fs::copy("fixtures/table.parquet", &input_copy).expect("failed to copy fixture");
        let input_copy_str = input_copy.to_str().expect("valid utf-8 path");

        let outcome = split_file(
            input_copy_str,
            None,
            input_copy_str,
            None,
            None,
            2,
            0,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert_eq!(outcome.partitions_written, 2);
        assert!(temp_dir.path().join("table.part00001.parquet").exists());
        assert!(temp_dir.path().join("table.part00002.parquet").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_json_input() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("out.json");
        let outcome = split_file(
            "fixtures/table.json",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            2,
            0,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert_eq!(outcome.partitions_written, 2);
        assert!(temp_dir.path().join("out.part00001.json").exists());
        assert!(temp_dir.path().join("out.part00002.json").exists());
    }
}
