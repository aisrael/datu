use anyhow::Result;
use anyhow::bail;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::FileType;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::avro;
use crate::pipeline::display;
use crate::pipeline::orc;
use crate::pipeline::xlsx;
use crate::utils::parse_select_columns;

/// Reads input file into a DataFusion DataFrame with optional column selection and limit.
pub async fn read_to_dataframe(
    input_path: &str,
    input_file_type: FileType,
    select: &Option<Vec<String>>,
    limit: Option<usize>,
) -> Result<datafusion::dataframe::DataFrame> {
    let ctx = SessionContext::new();

    let mut df = match input_file_type {
        FileType::Parquet => ctx
            .read_parquet(input_path, ParquetReadOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        FileType::Avro => ctx
            .read_avro(input_path, AvroReadOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
        FileType::Orc => {
            let batches = read_orc_to_batches(input_path)?;
            if batches.is_empty() {
                bail!("ORC file is empty or could not be read");
            }
            ctx.read_batches(batches)
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
        _ => bail!("Only Parquet, Avro, and ORC are supported as input file types"),
    };

    if let Some(columns) = select {
        let parsed = parse_select_columns(columns);
        if !parsed.is_empty() {
            let col_refs: Vec<&str> = parsed.iter().map(String::as_str).collect();
            df = df
                .select_columns(&col_refs)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    if let Some(n) = limit {
        df = df.limit(0, Some(n)).map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    Ok(df)
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion in read_to_dataframe.
fn read_orc_to_batches(path: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    use crate::pipeline::ReadArgs;

    let args = ReadArgs {
        path: path.to_string(),
        limit: None,
        offset: None,
    };
    let reader = orc::read_orc(&args)?;
    let batches: Vec<arrow::record_batch::RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(batches)
}

/// Writes a DataFusion DataFrame to the output file.
pub async fn write_dataframe(
    df: datafusion::dataframe::DataFrame,
    output_path: &str,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
) -> Result<()> {
    if output_file_type != FileType::Json && json_pretty {
        eprintln!("Warning: --json-pretty is only supported when converting to JSON");
    }

    match output_file_type {
        FileType::Parquet => {
            df.write_parquet(output_path, DataFrameWriteOptions::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        FileType::Csv => {
            df.write_csv(output_path, DataFrameWriteOptions::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        FileType::Json => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            let file = std::fs::File::create(output_path).map_err(anyhow::Error::from)?;
            if json_pretty {
                display::write_record_batches_as_json_pretty(&mut reader, file, sparse)?;
            } else {
                display::write_record_batches_as_json(&mut reader, file, sparse)?;
            }
        }
        FileType::Yaml => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            let file = std::fs::File::create(output_path).map_err(anyhow::Error::from)?;
            display::write_record_batches_as_yaml(&mut reader, file, sparse)?;
        }
        FileType::Avro => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            avro::write_record_batches(output_path, &mut reader)?;
        }
        FileType::Orc => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            orc::write_record_batches(output_path, &mut reader)?;
        }
        FileType::Xlsx => {
            let batches = df.collect().await.map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut reader = VecRecordBatchReader::new(batches);
            xlsx::write_record_batches(output_path, &mut reader)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
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

    // --- read_to_dataframe tests ---

    #[tokio::test]
    async fn test_read_to_dataframe() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_with_select() {
        let select = Some(vec!["one".to_string(), "two".to_string()]);
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &select, None)
            .await
            .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["one", "two"]);
        assert_eq!(count_rows(df).await, 3);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_with_limit() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, Some(2))
            .await
            .unwrap();
        assert_eq!(count_rows(df).await, 2);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_with_select_and_limit() {
        let select = Some(vec!["two".to_string()]);
        let df = read_to_dataframe(
            "fixtures/table.parquet",
            FileType::Parquet,
            &select,
            Some(1),
        )
        .await
        .unwrap();
        let schema = df.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["two"]);
        assert_eq!(count_rows(df).await, 1);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_avro() {
        let df = read_to_dataframe("fixtures/userdata5.avro", FileType::Avro, &None, Some(5))
            .await
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_orc() {
        let df = read_to_dataframe("fixtures/userdata.orc", FileType::Orc, &None, Some(5))
            .await
            .unwrap();
        assert_eq!(count_rows(df).await, 5);
    }

    #[tokio::test]
    async fn test_read_to_dataframe_unsupported_type() {
        let result = read_to_dataframe("fixtures/table.csv", FileType::Csv, &None, None).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only Parquet, Avro, and ORC")
        );
    }

    // --- write_dataframe tests ---

    #[tokio::test]
    async fn test_write_dataframe_to_parquet() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.parquet");
        write_dataframe(df, &output, FileType::Parquet, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = read_to_dataframe(&output, FileType::Parquet, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test]
    async fn test_write_dataframe_to_csv() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.csv");
        write_dataframe(df, &output, FileType::Csv, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    #[tokio::test]
    async fn test_write_dataframe_to_json() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_dataframe(df, &output, FileType::Json, true, false)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test]
    async fn test_write_dataframe_to_json_pretty() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.json");
        write_dataframe(df, &output, FileType::Json, true, true)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains('\n'));
        assert!(contents.contains("one"));
    }

    #[tokio::test]
    async fn test_write_dataframe_to_yaml() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.yaml");
        write_dataframe(df, &output, FileType::Yaml, true, false)
            .await
            .unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("one"));
    }

    #[tokio::test]
    async fn test_write_dataframe_to_avro() {
        let select = Some(vec!["two".to_string(), "three".to_string()]);
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &select, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.avro");
        write_dataframe(df, &output, FileType::Avro, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = read_to_dataframe(&output, FileType::Avro, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 3);
    }

    #[tokio::test]
    async fn test_write_dataframe_to_orc() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let df = read_to_dataframe("fixtures/userdata5.avro", FileType::Avro, &select, Some(5))
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.orc");
        write_dataframe(df, &output, FileType::Orc, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());

        let df2 = read_to_dataframe(&output, FileType::Orc, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df2).await, 5);
    }

    #[tokio::test]
    async fn test_write_dataframe_to_xlsx() {
        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_path(&temp_dir, "out.xlsx");
        write_dataframe(df, &output, FileType::Xlsx, true, false)
            .await
            .unwrap();
        assert!(std::path::Path::new(&output).exists());
    }

    // --- read_to_batches tests ---

    #[tokio::test]
    async fn test_read_to_batches_parquet() {
        let batches = read_to_batches("fixtures/table.parquet", FileType::Parquet, &None, None)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn test_read_to_batches_avro() {
        let batches = read_to_batches("fixtures/userdata5.avro", FileType::Avro, &None, Some(3))
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_read_to_batches_orc() {
        let batches = read_to_batches("fixtures/userdata.orc", FileType::Orc, &None, Some(3))
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    // --- write_batches tests ---

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn test_roundtrip_parquet_avro_parquet() {
        let select = Some(vec!["two".to_string(), "three".to_string()]);
        let temp_dir = tempfile::tempdir().unwrap();

        let df = read_to_dataframe("fixtures/table.parquet", FileType::Parquet, &select, None)
            .await
            .unwrap();
        let avro_path = temp_path(&temp_dir, "roundtrip.avro");
        write_dataframe(df, &avro_path, FileType::Avro, true, false)
            .await
            .unwrap();

        let df2 = read_to_dataframe(&avro_path, FileType::Avro, &None, None)
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        write_dataframe(df2, &parquet_path, FileType::Parquet, true, false)
            .await
            .unwrap();

        let df3 = read_to_dataframe(&parquet_path, FileType::Parquet, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df3).await, 3);
    }

    #[tokio::test]
    async fn test_roundtrip_avro_orc_parquet() {
        let select = Some(vec!["id".to_string(), "first_name".to_string()]);
        let temp_dir = tempfile::tempdir().unwrap();

        let df = read_to_dataframe("fixtures/userdata5.avro", FileType::Avro, &select, Some(5))
            .await
            .unwrap();
        let orc_path = temp_path(&temp_dir, "roundtrip.orc");
        write_dataframe(df, &orc_path, FileType::Orc, true, false)
            .await
            .unwrap();

        let df2 = read_to_dataframe(&orc_path, FileType::Orc, &None, None)
            .await
            .unwrap();
        let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
        write_dataframe(df2, &parquet_path, FileType::Parquet, true, false)
            .await
            .unwrap();

        let df3 = read_to_dataframe(&parquet_path, FileType::Parquet, &None, None)
            .await
            .unwrap();
        assert_eq!(count_rows(df3).await, 5);
    }
}
