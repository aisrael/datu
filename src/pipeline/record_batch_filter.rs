use arrow::array::RecordBatchReader;

use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Source;
use crate::pipeline::Step;

/// Pipeline step that filters record batches to only the specified columns.
pub struct SelectColumnsStep {
    pub prev: RecordBatchReaderSource,
    pub columns: Vec<String>,
}

impl Step for SelectColumnsStep {
    type Input = RecordBatchReaderSource;
    type Output = RecordBatchReaderSource;

    fn execute(self) -> crate::Result<Self::Output> {
        Ok(Box::new(self))
    }
}

impl Source<dyn RecordBatchReader + 'static> for SelectColumnsStep {
    fn get(&mut self) -> crate::Result<Box<dyn RecordBatchReader + 'static>> {
        let reader = self.prev.get()?;
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
        Ok(Box::new(SelectColumnRecordBatchReader {
            reader,
            schema: std::sync::Arc::new(projected_schema),
            indices,
        }) as Box<dyn RecordBatchReader + 'static>)
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

        let source = Box::new(parquet_step);
        let mut select_source = SelectColumnsStep {
            prev: source,
            columns: vec!["two".to_string(), "four".to_string()],
        };
        let mut projected_reader = select_source
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
