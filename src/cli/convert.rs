use crate::FileType;
use crate::pipeline::dataframe::DataFrameWriter;

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::pipeline::Source;
    use crate::pipeline::Step;
    use crate::pipeline::dataframe::DataFrameReader;
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
        let df = *DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .unwrap();
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select() {
        let select = Some(vec!["one".to_string(), "two".to_string()]);
        let df = *DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
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
        let df = *DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            Some(2),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_with_select_and_limit() {
        let select = Some(vec!["two".to_string()]);
        let df = *DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            Some(1),
            None,
        )
        .execute(())
        .await
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
        let df = *DataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            None,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap()
        .get()
        .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_orc() {
        let df = *DataFrameReader::new("fixtures/userdata.orc", FileType::Orc, None, Some(5), None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_csv() {
        let df = *DataFrameReader::new("fixtures/table.csv", FileType::Csv, None, Some(2), None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_dataframe_unsupported_type() {
        let result = DataFrameReader::new("fixtures/data.json", FileType::Json, None, None, None)
            .execute(())
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only Parquet, Avro, CSV, and ORC")
        );
    }

    // --- write_dataframe tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_parquet() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        DataFrameWriter::new(&output, FileType::Parquet, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *DataFrameReader::new(&output, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_csv() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        DataFrameWriter::new(&output, FileType::Csv, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        DataFrameWriter::new(&output, FileType::Json, true, false)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_json_pretty() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        DataFrameWriter::new(&output, FileType::Json, true, true)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains('\n'));
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_yaml() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.yaml");
        DataFrameWriter::new(&output, FileType::Yaml, true, false)
            .execute(source)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_avro() {
        let select = Some(vec!["two".to_string(), "three".to_string()]);
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.avro");
        DataFrameWriter::new(&output, FileType::Avro, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *DataFrameReader::new(&output, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_orc() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let source = DataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            select,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.orc");
        DataFrameWriter::new(&output, FileType::Orc, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = *DataFrameReader::new(&output, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df2).await, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_dataframe_to_xlsx() {
        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            None,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.xlsx");
        DataFrameWriter::new(&output, FileType::Xlsx, true, false)
            .execute(source)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    // --- read_to_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_parquet() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
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
            None,
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
        let batches = read_to_batches(
            "fixtures/userdata5.avro",
            FileType::Avro,
            &None,
            Some(3),
            None,
        )
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_to_batches_orc() {
        let batches = read_to_batches("fixtures/userdata.orc", FileType::Orc, &None, Some(3), None)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- write_batches tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_batches_to_csv() {
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
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
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
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
        let batches = read_to_batches(
            "fixtures/table.parquet",
            FileType::Parquet,
            &None,
            None,
            None,
        )
        .await
        .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        write_batches(batches, &output, FileType::Parquet, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let roundtrip = read_to_batches(&output, FileType::Parquet, &None, None, None)
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

        let source = DataFrameReader::new(
            "fixtures/table.parquet",
            FileType::Parquet,
            select,
            None,
            None,
        )
        .execute(())
        .await
        .unwrap();
        let avro_path = temp_path(&temp_dir, "roundtrip.avro");
        DataFrameWriter::new(&avro_path, FileType::Avro, true, false)
            .execute(source)
            .await
            .unwrap();

        let source2 = DataFrameReader::new(&avro_path, FileType::Avro, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *DataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_roundtrip_avro_orc_parquet() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let temp_dir = tempfile::tempdir().unwrap();

        let source = DataFrameReader::new(
            "fixtures/userdata5.avro",
            FileType::Avro,
            select,
            Some(5),
            None,
        )
        .execute(())
        .await
        .unwrap();
        let orc_path = temp_path(&temp_dir, "roundtrip.orc");
        DataFrameWriter::new(&orc_path, FileType::Orc, true, false)
            .execute(source)
            .await
            .unwrap();

        let source2 = DataFrameReader::new(&orc_path, FileType::Orc, None, None, None)
            .execute(())
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
            .execute(source2)
            .await
            .unwrap();

        let df3 = *DataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
            .execute(())
            .await
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(count_rows(df3).await, 5);
    }
}
