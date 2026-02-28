use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::Error;
use crate::FileType;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::avro;
use crate::pipeline::display;
use crate::pipeline::orc;
use crate::pipeline::parquet;
use crate::pipeline::xlsx;
use crate::utils::parse_select_columns;

/// A source that yields a DataFusion DataFrame, implementing `Source<DataFrame>`.
#[derive(Debug)]
pub struct DataFrameSource {
    df: Option<datafusion::dataframe::DataFrame>,
}

impl DataFrameSource {
    pub fn new(df: datafusion::dataframe::DataFrame) -> Self {
        Self { df: Some(df) }
    }
}

impl Source<datafusion::dataframe::DataFrame> for DataFrameSource {
    fn get(&mut self) -> crate::Result<Box<datafusion::dataframe::DataFrame>> {
        let df = self
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        Ok(Box::new(df))
    }
}

/// A step that reads an input file into a DataFusion DataFrame with optional column selection and limit.
pub struct DataFrameReader {
    input_path: String,
    input_file_type: FileType,
    select: Option<Vec<String>>,
    limit: Option<usize>,
}

impl DataFrameReader {
    pub fn new(
        input_path: &str,
        input_file_type: FileType,
        select: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            input_path: input_path.to_string(),
            input_file_type,
            select,
            limit,
        }
    }

    async fn read(&self) -> crate::Result<DataFrameSource> {
        let ctx = SessionContext::new();

        let mut df = match self.input_file_type {
            FileType::Parquet => ctx
                .read_parquet(&self.input_path, ParquetReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Avro => ctx
                .read_avro(&self.input_path, AvroReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Orc => {
                let batches = read_orc_to_batches(&self.input_path)?;
                if batches.is_empty() {
                    return Err(Error::GenericError(
                        "ORC file is empty or could not be read".to_string(),
                    ));
                }
                ctx.read_batches(batches)
                    .map_err(|e| Error::GenericError(e.to_string()))?
            }
            _ => {
                return Err(Error::GenericError(
                    "Only Parquet, Avro, and ORC are supported as input file types".to_string(),
                ));
            }
        };

        if let Some(columns) = &self.select {
            let parsed = parse_select_columns(columns);
            if !parsed.is_empty() {
                let col_refs: Vec<&str> = parsed.iter().map(String::as_str).collect();
                df = df
                    .select_columns(&col_refs)
                    .map_err(|e| Error::GenericError(e.to_string()))?;
            }
        }

        if let Some(n) = self.limit {
            df = df
                .limit(0, Some(n))
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }

        Ok(DataFrameSource::new(df))
    }
}

impl Step for DataFrameReader {
    type Input = ();
    type Output = DataFrameSource;

    fn execute(self, _input: Self::Input) -> crate::Result<Self::Output> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| handle.block_on(self.read()))
    }
}

/// Creates a `DataFrameReader` that reads an input file into a DataFusion DataFrame.
pub fn read_dataframe(
    input_path: &str,
    input_file_type: FileType,
    select: Option<Vec<String>>,
    limit: Option<usize>,
) -> DataFrameReader {
    DataFrameReader::new(input_path, input_file_type, select, limit)
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion after reading.
fn read_orc_to_batches(path: &str) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
    use crate::pipeline::ReadArgs;

    let args = ReadArgs {
        path: path.to_string(),
        limit: None,
        offset: None,
    };
    let reader = orc::read_orc(&args)?;
    let batches: Vec<arrow::record_batch::RecordBatch> =
        reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// A step that writes a DataFusion DataFrame to an output file.
pub struct DataFrameWriter {
    output_path: String,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
}

impl DataFrameWriter {
    pub fn new<S: Into<String>>(
        output_path: S,
        output_file_type: FileType,
        sparse: bool,
        json_pretty: bool,
    ) -> Self {
        Self {
            output_path: output_path.into(),
            output_file_type,
            sparse,
            json_pretty,
        }
    }
}

impl Step for DataFrameWriter {
    type Input = DataFrameSource;
    type Output = ();

    fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        if self.output_file_type != FileType::Json && self.json_pretty {
            eprintln!("Warning: --json-pretty is only supported when converting to JSON");
        }

        let df = input.get()?;

        let handle = tokio::runtime::Handle::current();
        let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))
            .map_err(|e| Error::GenericError(e.to_string()))?;

        let mut reader = VecRecordBatchReader::new(batches);
        let output_path = &self.output_path;

        match self.output_file_type {
            FileType::Parquet => {
                parquet::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Csv => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                let mut writer = arrow::csv::Writer::new(file);
                for batch in &mut reader {
                    let batch = batch.map_err(Error::ArrowError)?;
                    writer.write(&batch).map_err(Error::ArrowError)?;
                }
            }
            FileType::Json => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                if self.json_pretty {
                    display::write_record_batches_as_json_pretty(&mut reader, file, self.sparse)?;
                } else {
                    display::write_record_batches_as_json(&mut reader, file, self.sparse)?;
                }
            }
            FileType::Yaml => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                display::write_record_batches_as_yaml(&mut reader, file, self.sparse)?;
            }
            FileType::Avro => {
                avro::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Orc => {
                orc::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Xlsx => {
                xlsx::write_record_batches(output_path, &mut reader)?;
            }
        }

        Ok(())
    }
}

/// Creates a `DataFrameWriter` that writes a DataFusion DataFrame to the output file.
pub fn write_dataframe(
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> DataFrameWriter {
    DataFrameWriter {
        output_path: output_path.to_string(),
        output_file_type,
        sparse,
        json_pretty,
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::pipeline::Source;
    use crate::pipeline::read_to_batches;
    use crate::pipeline::write_batches;

    async fn count_rows(df: datafusion::dataframe::DataFrame) -> usize {
        let mut total_rows: usize = 0;
        let mut stream = df.execute_stream().await.unwrap();
        while let Some(batch) = stream.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        total_rows
    }

    fn temp_path(dir: &tempfile::TempDir, name: &str) -> String {
        dir.path()
            .join(name)
            .to_str()
            .expect("Failed to convert path to string")
            .to_string()
    }

    // --- DataFrameReader tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe() {
        let df = *read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select() {
        let select = Some(vec!["one".to_string(), "two".to_string()]);
        let df = *read_dataframe("fixtures/table.parquet", FileType::Parquet, select, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["one", "two"]);
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_limit() {
        let df = *read_dataframe("fixtures/table.parquet", FileType::Parquet, None, Some(2))
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select_and_limit() {
        let select = Some(vec!["two".to_string()]);
        let df = *read_dataframe("fixtures/table.parquet", FileType::Parquet, select, Some(1))
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["two"]);
        assert_eq!(count_rows(df).await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_avro() {
        let df = *read_dataframe("fixtures/userdata5.avro", FileType::Avro, None, Some(5))
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_orc() {
        let df = *read_dataframe("fixtures/userdata.orc", FileType::Orc, None, Some(5))
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_unsupported_type() {
        let result = read_dataframe("fixtures/table.csv", FileType::Csv, None, None).execute(());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only Parquet, Avro, and ORC")
        );
    }

    // --- write_dataframe tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_parquet() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        DataFrameWriter::new(&output, FileType::Parquet, true, false)
            .execute(source)
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *read_dataframe(&output, FileType::Parquet, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_csv() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        write_dataframe(&output, FileType::Csv, true, false)
            .execute(source)
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_dataframe(&output, FileType::Json, true, false)
            .execute(source)
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json_pretty() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_dataframe(&output, FileType::Json, true, true)
            .execute(source)
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains('\n'));
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_yaml() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.yaml");
        write_dataframe(&output, FileType::Yaml, true, false)
            .execute(source)
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_avro() {
        let select = Some(vec!["two".to_string(), "three".to_string()]);
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, select, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.avro");
        write_dataframe(&output, FileType::Avro, true, false)
            .execute(source)
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *read_dataframe(&output, FileType::Avro, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_orc() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let source = read_dataframe("fixtures/userdata5.avro", FileType::Avro, select, Some(5))
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.orc");
        write_dataframe(&output, FileType::Orc, true, false)
            .execute(source)
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *read_dataframe(&output, FileType::Orc, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_xlsx() {
        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, None, None)
            .execute(())
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.xlsx");
        write_dataframe(&output, FileType::Xlsx, true, false)
            .execute(source)
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    // --- read_to_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_parquet() {
        let batches = read_to_batches("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_with_select_and_limit() {
        let select = Some(vec!["two".to_string()]);
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &select,
            Some(2),
        )
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "two");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_avro() {
        let batches = read_to_batches("fixtures/userdata5.avro", FileType::Avro, &None, Some(3))
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_orc() {
        let batches = read_to_batches("fixtures/userdata.orc", FileType::Orc, &None, Some(3))
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- write_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_csv() {
        let batches = read_to_batches("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        write_batches(batches, &output, FileType::Csv, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_json() {
        let batches = read_to_batches("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_batches(batches, &output, FileType::Json, true, false)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_parquet() {
        let batches = read_to_batches("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        write_batches(batches, &output, FileType::Parquet, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let roundtrip = read_to_batches(&output, FileType::Parquet, &None, None)
            .await
            .unwrap();
        let total_rows: usize = roundtrip.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- round-trip tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_parquet_avro_parquet() {
        let select = Some(vec!["two".to_string(), "three".to_string()]);
        let temp_dir = tempfile::tempdir().unwrap();

        let source = read_dataframe("fixtures/table.parquet", FileType::Parquet, select, None)
            .execute(())
            .unwrap();
        let avro_path = temp_path(&temp_dir, "roundtrip.avro");
        write_dataframe(&avro_path, FileType::Avro, true, false)
            .execute(source)
            .unwrap();

        let source2 = read_dataframe(&avro_path, FileType::Avro, None, None)
            .execute(())
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        write_dataframe(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .unwrap();

        let df3 = *read_dataframe(&parquet_path, FileType::Parquet, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_avro_orc_parquet() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let temp_dir = tempfile::tempdir().unwrap();

        let source = read_dataframe("fixtures/userdata5.avro", FileType::Avro, select, Some(5))
            .execute(())
            .unwrap();
        let orc_path = temp_path(&temp_dir, "roundtrip.orc");
        write_dataframe(&orc_path, FileType::Orc, true, false)
            .execute(source)
            .unwrap();

        let source2 = read_dataframe(&orc_path, FileType::Orc, None, None)
            .execute(())
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        write_dataframe(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .unwrap();

        let df3 = *read_dataframe(&parquet_path, FileType::Parquet, None, None)
            .execute(())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 5);
    }
}
