//! In-memory tail, index sampling, and reservoir sampling over record batches.

use arrow::array::RecordBatchReader;
use arrow::array::UInt32Array;
use arrow::compute;
use arrow::record_batch::RecordBatch;

/// Takes the last `n` rows from a sequence of record batches.
pub fn tail_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    n: usize,
) -> Vec<arrow::record_batch::RecordBatch> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let number = n.min(total_rows);
    let skip = total_rows.saturating_sub(number);

    let mut result = Vec::new();
    let mut rows_emitted = 0usize;
    let mut rows_skipped = 0usize;
    for batch in batches {
        let batch_rows = batch.num_rows();
        if rows_skipped + batch_rows <= skip {
            rows_skipped += batch_rows;
            continue;
        }
        let start_in_batch = skip.saturating_sub(rows_skipped);
        rows_skipped += start_in_batch;
        let take = (number - rows_emitted).min(batch_rows - start_in_batch);
        if take == 0 {
            break;
        }
        result.push(batch.slice(start_in_batch, take));
        rows_emitted += take;
    }

    result
}

/// Extracts pre-chosen rows from a single batch given sorted global indices.
/// Advances `idx_pos` past all consumed indices and returns the selected rows (if any).
pub(super) fn take_rows_at_sorted_indices(
    batch: &RecordBatch,
    batch_start: usize,
    indices: &[usize],
    idx_pos: &mut usize,
) -> Option<RecordBatch> {
    let batch_end = batch_start + batch.num_rows();
    let mut local_indices: Vec<u32> = Vec::new();
    while *idx_pos < indices.len() && indices[*idx_pos] < batch_end {
        local_indices.push((indices[*idx_pos] - batch_start) as u32);
        *idx_pos += 1;
    }
    if local_indices.is_empty() {
        return None;
    }
    let index_array = UInt32Array::from(local_indices);
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col, &index_array, None).expect("take failed"))
        .collect();
    Some(RecordBatch::try_new(batch.schema(), columns).expect("RecordBatch::try_new failed"))
}

/// Samples `n` random rows by streaming through a reader when `total_rows` is known
/// (Parquet, ORC). Generates sorted random indices upfront, then picks rows batch-by-batch
/// without holding all records in memory.
pub fn sample_from_reader(
    reader: Box<dyn RecordBatchReader + 'static>,
    total_rows: usize,
    n: usize,
) -> Vec<RecordBatch> {
    let effective_n = n.min(total_rows);
    let mut rng = rand::rng();
    let mut indices: Vec<usize> =
        rand::seq::index::sample(&mut rng, total_rows, effective_n).into_vec();
    indices.sort_unstable();

    let mut result = Vec::new();
    let mut batch_start = 0usize;
    let mut idx_pos = 0usize;
    for batch_result in reader {
        if idx_pos >= indices.len() {
            break;
        }
        let batch = match batch_result {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(selected) =
            take_rows_at_sorted_indices(&batch, batch_start, &indices, &mut idx_pos)
        {
            result.push(selected);
        }
        batch_start += batch.num_rows();
    }
    result
}

/// Samples `n` rows from a reader using reservoir sampling (Algorithm L).
/// Used for formats where the total row count is unknown (Avro, CSV).
pub fn reservoir_sample_from_reader(
    reader: Box<dyn RecordBatchReader + 'static>,
    n: usize,
) -> Vec<RecordBatch> {
    let schema = reader.schema();
    let row_iter = reader.filter_map(|r| r.ok()).flat_map(|batch| {
        let num = batch.num_rows();
        (0..num).map(move |i| Some(batch.slice(i, 1)))
    });
    let mut sample: Vec<Option<RecordBatch>> = vec![None; n];
    reservoir_sampling::unweighted::l(row_iter, &mut sample);
    let sampled: Vec<RecordBatch> = sample.into_iter().flatten().collect();
    if sampled.is_empty() {
        return sampled;
    }
    match compute::concat_batches(&schema, &sampled) {
        Ok(merged) => vec![merged],
        Err(_) => sampled,
    }
}
