//! Splits a single input file into multiple output files of at most N rows (or bytes) each
//! (`datu split`).

use std::path::Path;
use std::str::FromStr;

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
use crate::pipeline::record_batch::apply_offset_limit;
use crate::pipeline::step::Producer;
use crate::pipeline::write::write_record_batches_from_reader;
use crate::resolve_file_type;

/// The size of each output partition: either a row count, or an (approximate) byte size.
///
/// A plain integer (e.g. `"100000"`) is a row count. A number followed by a unit is a byte size:
/// `kb`/`mb`/`gb`/`tb` are decimal (SI, base 1000) and `kib`/`mib`/`gib`/`tib` are binary (IEC,
/// base 1024); units are case-insensitive. Byte sizes are necessarily approximate — see
/// [`PartitionReader`]'s `Bytes` handling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitSize {
    Rows(usize),
    Bytes(u64),
}

impl SplitSize {
    fn is_zero(self) -> bool {
        match self {
            SplitSize::Rows(n) => n == 0,
            SplitSize::Bytes(n) => n == 0,
        }
    }
}

impl TryFrom<&str> for SplitSize {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        let trimmed = s.trim();
        if let Ok(rows) = trimmed.parse::<usize>() {
            return Ok(SplitSize::Rows(rows));
        }

        let split_at = trimmed
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| format!("invalid --split value '{s}'"))?;
        let (magnitude, unit) = trimmed.split_at(split_at);
        let magnitude: f64 = magnitude
            .parse()
            .map_err(|_| format!("invalid --split value '{s}'"))?;
        if !magnitude.is_finite() || magnitude < 0.0 {
            return Err(format!("invalid --split value '{s}'"));
        }

        let multiplier: f64 = match unit.trim().to_lowercase().as_str() {
            "kb" => 1_000.0,
            "mb" => 1_000_000.0,
            "gb" => 1_000_000_000.0,
            "tb" => 1_000_000_000_000.0,
            "kib" => 1024.0,
            "mib" => 1024.0 * 1024.0,
            "gib" => 1024.0 * 1024.0 * 1024.0,
            "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
            other => {
                return Err(format!(
                    "unknown size unit '{other}', expected one of: kb, mb, gb, tb, kib, mib, gib, tib"
                ));
            }
        };

        Ok(SplitSize::Bytes((magnitude * multiplier).round() as u64))
    }
}

impl FromStr for SplitSize {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl std::fmt::Display for SplitSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitSize::Rows(n) => write!(f, "{n}"),
            SplitSize::Bytes(n) => write!(f, "{n}"),
        }
    }
}

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

/// Remaining budget for the partition currently being written.
#[derive(Clone, Copy)]
enum RemainingBudget {
    Rows(usize),
    Bytes(u64),
}

impl RemainingBudget {
    fn is_exhausted(self) -> bool {
        match self {
            RemainingBudget::Rows(n) => n == 0,
            RemainingBudget::Bytes(n) => n == 0,
        }
    }
}

/// Streams a bounded number of rows (or bytes, estimated from in-memory Arrow batch size) per
/// partition from a shared underlying reader. When a source batch spans a partition boundary, the
/// leftover slice is buffered in `pending` and consumed by the *next* partition instead of being
/// dropped.
struct PartitionReader<'a> {
    inner: &'a mut dyn RecordBatchReader,
    schema: SchemaRef,
    pending: Option<RecordBatch>,
    remaining: RemainingBudget,
    consumed: usize,
}

impl<'a> PartitionReader<'a> {
    fn new(inner: &'a mut dyn RecordBatchReader) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            pending: None,
            remaining: RemainingBudget::Rows(0),
            consumed: 0,
        }
    }

    /// Resets the row/byte budget and consumed counter for the next partition.
    fn start_partition(&mut self, size: SplitSize) {
        self.remaining = match size {
            SplitSize::Rows(n) => RemainingBudget::Rows(n),
            SplitSize::Bytes(n) => RemainingBudget::Bytes(n),
        };
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

    /// Stashes the unconsumed tail of `batch` (rows `take..`) for the next partition, unless
    /// `take` covers the whole batch — a zero-row `pending` batch would otherwise make
    /// [`Self::has_more`] falsely report more data and produce a phantom empty partition.
    fn set_leftover(&mut self, batch: &RecordBatch, take: usize, rows: usize) {
        if take < rows {
            self.pending = Some(batch.slice(take, rows - take));
        }
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
        if self.remaining.is_exhausted() {
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
            match self.remaining {
                RemainingBudget::Rows(remaining_rows) => {
                    if rows <= remaining_rows {
                        self.remaining = RemainingBudget::Rows(remaining_rows - rows);
                        self.consumed += rows;
                        return Some(Ok(batch));
                    }
                    let take = remaining_rows;
                    self.set_leftover(&batch, take, rows);
                    self.consumed += take;
                    self.remaining = RemainingBudget::Rows(0);
                    return Some(Ok(batch.slice(0, take)));
                }
                RemainingBudget::Bytes(remaining_bytes) => {
                    let batch_bytes = batch.get_array_memory_size() as u64;
                    if batch_bytes <= remaining_bytes {
                        self.remaining = RemainingBudget::Bytes(remaining_bytes - batch_bytes);
                        self.consumed += rows;
                        return Some(Ok(batch));
                    }
                    // Estimate rows-per-byte from this batch and slice at the boundary; always
                    // take at least one row so a partition can never get stuck making no progress.
                    let per_row = (batch_bytes / rows as u64).max(1);
                    let take = ((remaining_bytes / per_row) as usize).clamp(1, rows);
                    self.set_leftover(&batch, take, rows);
                    self.consumed += take;
                    self.remaining = RemainingBudget::Bytes(0);
                    return Some(Ok(batch.slice(0, take)));
                }
            }
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

/// Reads `input_path` once and writes consecutive chunks of at most `split_size` rows (or bytes)
/// to numbered partition files derived from `output_base` (see [`partition_output_path`]). `limit`
/// caps the total number of rows processed across all partitions (`0` means unlimited).
#[allow(clippy::too_many_arguments)]
pub async fn split_file(
    input_path: &str,
    input_type_override: Option<FileType>,
    output_base: &str,
    output_type_override: Option<FileType>,
    csv_has_header: Option<bool>,
    split_size: SplitSize,
    limit: usize,
    sparse: bool,
    json_pretty: bool,
    progress: Option<ProgressBar>,
) -> Result<SplitOutcome> {
    if split_size.is_zero() {
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

    let reader = open_row_reader(input_path, input_file_type, csv_has_header).await?;
    let mut reader = apply_offset_limit(reader, 0, (limit > 0).then_some(limit));
    let mut partition_reader = PartitionReader::new(&mut *reader);

    let mut partition_index: usize = 1;
    let mut rows_written: usize = 0;

    while partition_reader.has_more()? {
        partition_reader.start_partition(split_size);
        let path = partition_output_path(output_base, output_file_type, partition_index);
        write_record_batches_from_reader(
            &mut partition_reader,
            &path,
            output_file_type,
            sparse,
            json_pretty,
        )?;

        rows_written += partition_reader.consumed;
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
    fn test_split_size_parses_plain_rows() {
        assert_eq!(
            SplitSize::try_from("100000").unwrap(),
            SplitSize::Rows(100_000)
        );
        assert_eq!(SplitSize::try_from("0").unwrap(), SplitSize::Rows(0));
    }

    #[test]
    fn test_split_size_parses_decimal_units() {
        assert_eq!(SplitSize::try_from("1kb").unwrap(), SplitSize::Bytes(1_000));
        assert_eq!(
            SplitSize::try_from("64mb").unwrap(),
            SplitSize::Bytes(64_000_000)
        );
        assert_eq!(
            SplitSize::try_from("2gb").unwrap(),
            SplitSize::Bytes(2_000_000_000)
        );
        assert_eq!(
            SplitSize::try_from("1tb").unwrap(),
            SplitSize::Bytes(1_000_000_000_000)
        );
    }

    #[test]
    fn test_split_size_parses_binary_units() {
        assert_eq!(SplitSize::try_from("1kib").unwrap(), SplitSize::Bytes(1024));
        assert_eq!(
            SplitSize::try_from("1mib").unwrap(),
            SplitSize::Bytes(1_048_576)
        );
        assert_eq!(
            SplitSize::try_from("1gib").unwrap(),
            SplitSize::Bytes(1_073_741_824)
        );
        assert_eq!(
            SplitSize::try_from("1tib").unwrap(),
            SplitSize::Bytes(1_099_511_627_776)
        );
        assert_eq!(
            SplitSize::try_from("1.5gib").unwrap(),
            SplitSize::Bytes(1_610_612_736)
        );
    }

    #[test]
    fn test_split_size_case_insensitive() {
        assert_eq!(
            SplitSize::try_from("10KB").unwrap(),
            SplitSize::Bytes(10_000)
        );
        assert_eq!(
            SplitSize::try_from("10Kb").unwrap(),
            SplitSize::Bytes(10_000)
        );
        assert_eq!(
            SplitSize::try_from("1MiB").unwrap(),
            SplitSize::Bytes(1_048_576)
        );
    }

    #[test]
    fn test_split_size_rejects_unknown_unit() {
        let err = SplitSize::try_from("10xb").unwrap_err();
        assert!(err.contains("unknown size unit"), "unexpected error: {err}");
    }

    #[test]
    fn test_split_size_rejects_malformed_number() {
        assert!(SplitSize::try_from("mb").is_err());
        assert!(SplitSize::try_from("1.5.5mb").is_err());
        assert!(SplitSize::try_from("-5mb").is_err());
    }

    #[test]
    fn test_split_size_zero_with_unit_parses_but_is_zero() {
        let size = SplitSize::try_from("0mb").expect("should parse");
        assert_eq!(size, SplitSize::Bytes(0));
        assert!(size.is_zero());
    }

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
            SplitSize::Rows(0),
            0,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_err(), "split with split_size 0 should fail");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_rejects_zero_byte_split_size() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("out.parquet");
        let result = split_file(
            "fixtures/table.parquet",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            SplitSize::Bytes(0),
            0,
            true,
            false,
            None,
        )
        .await;
        assert!(result.is_err(), "split with byte split_size 0 should fail");
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
            SplitSize::Rows(2),
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
            SplitSize::Rows(100),
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
            SplitSize::Rows(2),
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
            SplitSize::Rows(2),
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

    /// A byte budget small relative to `fixtures/userdata5.avro`'s on-disk size (1000 rows), used
    /// to force multiple partitions without depending on the exact in-memory-vs-on-disk size
    /// ratio (in-memory Arrow representation is not directly comparable to Avro's binary encoding).
    fn small_byte_budget_for_userdata5() -> SplitSize {
        let file_bytes = std::fs::metadata("fixtures/userdata5.avro")
            .expect("failed to stat fixture")
            .len();
        SplitSize::Bytes(file_bytes / 5)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_by_byte_size() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("u.avro");
        let outcome = split_file(
            "fixtures/userdata5.avro",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            small_byte_budget_for_userdata5(),
            0,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert!(
            outcome.partitions_written >= 2,
            "expected multiple partitions, got {}",
            outcome.partitions_written
        );
        assert_eq!(outcome.rows_written, 1000);

        let mut total_rows = 0usize;
        for index in 1..=outcome.partitions_written {
            let path = temp_dir.path().join(format!("u.part{index:05}.avro"));
            assert!(path.exists(), "partition {index} should exist");
            let rows = crate::pipeline::count_rows(
                path.to_str().expect("valid utf-8 path"),
                FileType::Avro,
                None,
            )
            .await
            .expect("failed to count rows");
            assert!(rows > 0, "partition {index} should have at least 1 row");
            total_rows += rows;
        }
        assert_eq!(total_rows, 1000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_by_byte_size_respects_limit() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_base = temp_dir.path().join("u.avro");
        let outcome = split_file(
            "fixtures/userdata5.avro",
            None,
            output_base.to_str().expect("valid utf-8 path"),
            None,
            None,
            small_byte_budget_for_userdata5(),
            250,
            true,
            false,
            None,
        )
        .await
        .expect("split should succeed");

        assert_eq!(outcome.rows_written, 250);
    }
}
