use arrow::array::RecordBatchReader;

use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;

pub struct SelectColumnsStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub columns: Vec<String>,
}

impl Step for SelectColumnsStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = Box<dyn RecordBatchReaderSource>;

    fn execute(self) -> crate::Result<Self::Output> {
        let select_column_reader_source = SelectColumnRecordBatchReaderSource {
            input: self.prev,
            columns: self.columns.clone(),
        };
        Ok(Box::new(select_column_reader_source))
    }
}

pub struct SelectColumnRecordBatchReaderSource {
    input: Box<dyn RecordBatchReaderSource>,
    columns: Vec<String>,
}

impl RecordBatchReaderSource for SelectColumnRecordBatchReaderSource {
    fn get_record_batch_reader(&mut self) -> crate::Result<Box<dyn RecordBatchReader>> {
        let reader = self.input.get_record_batch_reader()?;
        let schema = reader.schema();

        let mut indices = Vec::new();
        for col_name in &self.columns {
            match schema.index_of(col_name) {
                Ok(idx) => indices.push(idx),
                Err(_) => {
                    return Err(crate::Error::GenericError(format!(
                        "Column '{}' not found in schema",
                        col_name
                    )));
                }
            }
        }

        let projected_schema = std::sync::Arc::new(schema.project(&indices)?);

        let select_column_reader = SelectColumnRecordBatchReader {
            reader,
            schema: projected_schema,
            indices,
        };
        Ok(Box::new(select_column_reader))
    }
}

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
    use crate::pipeline::parquet::ReadParquetArgs;
    use crate::pipeline::parquet::ReadParquetStep;

    #[test]
    fn test_select_columns() {
        // Use the parquet reader to inspect the file and verify column selection
        let args = ReadParquetArgs {
            path: "fixtures/table.parquet".to_string(),
            limit: None,
        };
        let parquet_step = ReadParquetStep { args };

        let source = Box::new(parquet_step);
        let mut select_source = SelectColumnRecordBatchReaderSource {
            input: source,
            columns: vec!["two".to_string(), "four".to_string()],
        };

        let mut projected_reader = select_source.get_record_batch_reader().unwrap();

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
