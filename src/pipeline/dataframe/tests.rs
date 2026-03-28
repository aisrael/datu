use futures::StreamExt;

use super::DataFrameWriter;
use super::DataframeSelect;
use super::DataframeTail;
use super::DataframeToRecordBatchProducer;
use super::LegacyDataFrameReader;
use crate::FileType;
use crate::pipeline::DataframeParquetReader;
use crate::pipeline::Producer;
use crate::pipeline::RecordBatchAvroWriter;
use crate::pipeline::SelectSpec;
use crate::pipeline::Step;
use crate::pipeline::csv::DataframeCsvWriter;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::read_to_batches;
use crate::pipeline::write::WriteArgs;
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
    let df = *LegacyDataFrameReader::new(
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
    .await
    .unwrap();
    assert_eq!(count_rows(df).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_with_select() {
    let select = SelectSpec::from_cli_args(&Some(vec!["one".to_string(), "two".to_string()]));
    let df = *LegacyDataFrameReader::new(
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
    .await
    .unwrap();
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["one", "two"]);
    assert_eq!(count_rows(df).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_with_limit() {
    let df = *LegacyDataFrameReader::new(
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
    .await
    .unwrap();
    assert_eq!(count_rows(df).await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_with_select_and_limit() {
    let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string()]));
    let df = *LegacyDataFrameReader::new(
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
    .await
    .unwrap();
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["two"]);
    assert_eq!(count_rows(df).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_avro() {
    let df = *LegacyDataFrameReader::new(
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
    .await
    .unwrap();
    assert_eq!(count_rows(df).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_orc() {
    let df =
        *LegacyDataFrameReader::new("fixtures/userdata.orc", FileType::Orc, None, Some(5), None)
            .execute(())
            .await
            .unwrap()
            .get()
            .await
            .unwrap();
    assert_eq!(count_rows(df).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_csv() {
    let df = *LegacyDataFrameReader::new("fixtures/table.csv", FileType::Csv, None, Some(2), None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df).await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_dataframe_unsupported_type() {
    let result =
        LegacyDataFrameReader::new("fixtures/table.parquet", FileType::Xlsx, None, None, None)
            .execute(())
            .await;
    match result {
        Ok(_) => panic!("Expected error, got Ok"),
        Err(error) => assert!(
            error
                .to_string()
                .contains("Only Parquet, Avro, CSV, JSON, and ORC")
        ),
    }
}

// --- write_dataframe tests ---

#[tokio::test(flavor = "multi_thread")]
async fn test_write_dataframe_to_parquet() {
    let source = LegacyDataFrameReader::new(
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

    let df2 = *LegacyDataFrameReader::new(&output, FileType::Parquet, None, None, None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df2).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_dataframe_to_csv() {
    let source = LegacyDataFrameReader::new(
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
    let source = LegacyDataFrameReader::new(
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
    let source = LegacyDataFrameReader::new(
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
    let source = LegacyDataFrameReader::new(
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
    let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string(), "three".to_string()]));
    let source = LegacyDataFrameReader::new(
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

    let df2 = *LegacyDataFrameReader::new(&output, FileType::Avro, None, None, None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df2).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_dataframe_to_orc() {
    let select = SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
    let source = LegacyDataFrameReader::new(
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

    let df2 = *LegacyDataFrameReader::new(&output, FileType::Orc, None, None, None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df2).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_dataframe_to_xlsx() {
    let source = LegacyDataFrameReader::new(
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
    let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string()]));
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
    let select = SelectSpec::from_cli_args(&Some(vec!["two".to_string(), "three".to_string()]));
    let temp_dir = tempfile::tempdir().unwrap();

    let source = LegacyDataFrameReader::new(
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

    let source2 = LegacyDataFrameReader::new(&avro_path, FileType::Avro, None, None, None)
        .execute(())
        .await
        .unwrap();
    let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
    DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
        .execute(source2)
        .await
        .unwrap();

    let df3 = *LegacyDataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df3).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_roundtrip_avro_orc_parquet() {
    let select = SelectSpec::from_cli_args(&Some(vec!["id".to_string(), "first_name".to_string()]));
    let temp_dir = tempfile::tempdir().unwrap();

    let source = LegacyDataFrameReader::new(
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

    let source2 = LegacyDataFrameReader::new(&orc_path, FileType::Orc, None, None, None)
        .execute(())
        .await
        .unwrap();
    let parquet_path = temp_path(&temp_dir, "roundtrip.parquet");
    DataFrameWriter::new(&parquet_path, FileType::Parquet, true, false)
        .execute(source2)
        .await
        .unwrap();

    let df3 = *LegacyDataFrameReader::new(&parquet_path, FileType::Parquet, None, None, None)
        .execute(())
        .await
        .unwrap()
        .get()
        .await
        .unwrap();
    assert_eq!(count_rows(df3).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_steps_parquet_tail_to_csv() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let source = DataframeSelect { select: None }
        .execute(source)
        .await
        .unwrap();
    let source = DataframeTail {
        input_path: "fixtures/table.parquet".to_string(),
        input_file_type: FileType::Parquet,
        n: 2,
    }
    .execute(source)
    .await
    .unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let output = temp_path(&temp_dir, "out.csv");
    let write_args = WriteArgs {
        path: output.clone(),
        file_type: FileType::Csv,
        sparse: None,
        pretty: None,
    };
    DataframeCsvWriter { args: write_args }
        .execute(Box::new(source))
        .await
        .unwrap();
    assert!(std::path::Path::new(&output).exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_to_record_batch_record_batch_avro_writer() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let producer = DataframeToRecordBatchProducer::new(source);
    let temp_dir = tempfile::tempdir().unwrap();
    let output = temp_path(&temp_dir, "out.avro");
    let write_args = WriteArgs {
        path: output.clone(),
        file_type: FileType::Avro,
        sparse: None,
        pretty: None,
    };
    RecordBatchAvroWriter { args: write_args }
        .execute(Box::new(producer))
        .await
        .unwrap();
    assert!(std::path::Path::new(&output).exists());
}
