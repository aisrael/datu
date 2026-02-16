use arrow::array::RecordBatchReader;

use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;

/// A Source that wraps a single RecordBatchReader and yields it on get().
struct RecordBatchReaderHolder {
    reader: Option<Box<dyn RecordBatchReader + 'static>>,
}

impl Source<dyn RecordBatchReader + 'static> for RecordBatchReaderHolder {
    fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        std::mem::take(&mut self.reader)
            .ok_or_else(|| crate::Error::GenericError("Reader already taken".to_string()))
    }
}

/// Pipeline step that filters record batches to only the specified columns.
pub struct SelectColumnsStep {
    pub columns: Vec<String>,
}

impl Step for SelectColumnsStep {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        let reader = input.get()?;
        let indices: Vec<usize> = self
            .columns
            .iter()
            .map(|col| {
                reader.schema().index_of(col).map_err(|e| {
                    crate::Error::GenericError(format!("Column '{col}' not found: {e}"))
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let projected_schema = reader.schema().project(&indices)?;
        let projected_reader = SelectColumnRecordBatchReader {
            reader,
            schema: std::sync::Arc::new(projected_schema),
            indices,
        };
        Ok(Box::new(RecordBatchReaderHolder {
            reader: Some(Box::new(projected_reader)),
        }))
    }
}

/// Record batch reader that projects only the selected column indices.
pub struct SelectColumnRecordBatchReader {
    reader: Box<dyn RecordBatchReader>,
    schema: arrow::datatypes::SchemaRef,
    indices: Vec<usize>,
}

impl RecordBatchReader for SelectColumnRecordBatchReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for SelectColumnRecordBatchReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader
            .next()
            .map(|batch| batch.and_then(|b| b.project(&self.indices)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ReadArgs;
    use crate::pipeline::RecordBatchReaderSource;
    use crate::pipeline::parquet::ReadParquetStep;

    #[test]
    fn test_select_columns() {
        // Use the parquet reader to inspect the file and verify column selection
        let args = ReadArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: None,
            offset: None,
        };
        let parquet_step = ReadParquetStep { args };

        let source: RecordBatchReaderSource = Box::new(parquet_step);
        let select_step = SelectColumnsStep {
            columns: vec!["two".to_string(), "four".to_string()],
        };
        let mut projected_source = select_step
            .execute(source)
            .expect("Failed to execute select columns");
        let mut projected_reader = projected_source
            .get()
            .expect("Failed to get record batch reader");

        // 1. Check Schema
        let projected_schema = projected_reader.schema();
        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "two");
        assert_eq!(projected_schema.field(1).name(), "four");

        // 2. Check Data
        let batch_result = projected_reader.next().unwrap();
        let projected_batch = batch_result.unwrap();
        let batch_rows = projected_batch.num_rows();

        assert_eq!(projected_batch.num_columns(), 2);
        assert_eq!(projected_batch.column(0).len(), batch_rows);
        assert_eq!(projected_batch.column(1).len(), batch_rows);
    }
}
