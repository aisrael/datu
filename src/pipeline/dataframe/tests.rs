use arrow::array::RecordBatchReader;

use super::DataframeSelect;
use super::DataframeTail;
use crate::FileType;
use crate::pipeline::ColumnSpec;
use crate::pipeline::DataframeParquetReader;
use crate::pipeline::DataframeToRecordBatch;
use crate::pipeline::GroupByKey;
use crate::pipeline::RecordBatchAvroWriter;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;
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

async fn schema_field_names(source: crate::pipeline::DataFrameSource) -> Vec<String> {
    let reader = DataframeToRecordBatch::try_new(source).await.unwrap();
    reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_select_plain_projection_with_alias() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
            SelectItem::column(ColumnSpec::CaseInsensitive("three".into()))
                .with_alias("three_alias"),
        ],
        group_by: None,
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["two".to_string(), "three_alias".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_select_global_aggregate_with_alias() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::sum(ColumnSpec::CaseInsensitive("one".into())).with_alias("total"),
        ],
        group_by: None,
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["total".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_select_group_by_aliases_key_and_aggregate() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("two".into())).with_alias("two_alias"),
            SelectItem::sum(ColumnSpec::CaseInsensitive("one".into())).with_alias("total"),
        ],
        group_by: Some(vec![GroupByKey::new(ColumnSpec::CaseInsensitive(
            "two".into(),
        ))]),
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["two_alias".to_string(), "total".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_group_by_alias_used_when_select_has_no_alias() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("two".into())),
            SelectItem::sum(ColumnSpec::CaseInsensitive("one".into())).with_alias("total"),
        ],
        group_by: Some(vec![
            GroupByKey::new(ColumnSpec::CaseInsensitive("two".into())).with_alias("two_alias"),
        ]),
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["two_alias".to_string(), "total".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_select_alias_overrides_group_by_alias() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("two".into())).with_alias("from_select"),
            SelectItem::sum(ColumnSpec::CaseInsensitive("one".into())).with_alias("total"),
        ],
        group_by: Some(vec![
            GroupByKey::new(ColumnSpec::CaseInsensitive("two".into())).with_alias("from_group_by"),
        ]),
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["from_select".to_string(), "total".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_select_references_group_key_by_group_by_alias() {
    // select() may reference a group_by() key by its alias instead of its underlying column.
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let source = DataframeParquetReader { args: read_args }
        .execute(())
        .await
        .unwrap();
    let spec = SelectSpec {
        columns: vec![
            SelectItem::column(ColumnSpec::CaseInsensitive("key".into())),
            SelectItem::sum(ColumnSpec::CaseInsensitive("one".into())).with_alias("total"),
        ],
        group_by: Some(vec![
            GroupByKey::new(ColumnSpec::CaseInsensitive("two".into())).with_alias("key"),
        ]),
    };
    let source = DataframeSelect { select: Some(spec) }
        .execute(source)
        .await
        .unwrap();
    let names = schema_field_names(source).await;
    assert_eq!(names, vec!["key".to_string(), "total".to_string()]);
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
