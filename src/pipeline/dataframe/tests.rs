use super::DataframeSelect;
use super::DataframeTail;
use crate::FileType;
use crate::pipeline::DataframeParquetReader;
use crate::pipeline::DataframeToRecordBatch;
use crate::pipeline::RecordBatchAvroWriter;
use crate::pipeline::Step;
use crate::pipeline::csv::DataframeCsvWriter;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::write::WriteArgs;

fn temp_path(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path()
        .join(name)
        .to_str()
        .expect("Failed to convert path to string")
        .to_string()
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
    let reader = DataframeToRecordBatch::try_new(source).await.unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let output = temp_path(&temp_dir, "out.avro");
    let write_args = WriteArgs {
        path: output.clone(),
        file_type: FileType::Avro,
        sparse: None,
        pretty: None,
    };
    RecordBatchAvroWriter { args: write_args }
        .execute(Box::new(reader))
        .await
        .unwrap();
    assert!(std::path::Path::new(&output).exists());
}
