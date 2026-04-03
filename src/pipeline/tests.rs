use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use tempfile::NamedTempFile;

use super::*;
use crate::Error;
use crate::FileType;
use crate::pipeline::ColumnSpec;
use crate::pipeline::DataframeParquetReader;
use crate::pipeline::SelectItem;
use crate::pipeline::SelectSpec;
use crate::pipeline::avro::DataframeAvroWriter;
use crate::pipeline::avro::get_schema_fields_avro;
use crate::pipeline::dataframe::DataFrameSink;
use crate::pipeline::read::ReadArgs;
use crate::pipeline::write::WriteArgs;
use crate::select_spec;

#[test]
fn test_pipeline_builder_read_select_write_parquet() {
    let mut builder = PipelineBuilder::new();
    builder
        .read("input.parquet")
        .select(&["one", "two"])
        .write("output.parquet");
    let built = builder.build().unwrap();
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(FileType::Parquet, p.input_file_type);
    assert_eq!(Some(select_spec!("one", "two")), p.select);
    let DataFrameSink::Write {
        output_file_type, ..
    } = &p.sink
    else {
        panic!("expected write sink");
    };
    assert_eq!(FileType::Parquet, *output_file_type);
}

#[test]
fn test_pipeline_builder_read_select_write_csv() {
    let mut builder = PipelineBuilder::new();
    builder
        .read("input.parquet")
        .select(&["one", "two"])
        .write("output.csv");

    let built = builder.build().expect("Failed to build pipeline");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(FileType::Parquet, p.input_file_type);
    assert_eq!(Some(select_spec!("one", "two")), p.select);
    let DataFrameSink::Write {
        output_file_type, ..
    } = &p.sink
    else {
        panic!("expected write sink");
    };
    assert_eq!(FileType::Csv, *output_file_type);
}

#[test]
fn test_pipeline_builder_read_orc_write_csv() {
    let mut builder = PipelineBuilder::new();
    builder
        .read("input.orc")
        .select(&["col"])
        .write("output.csv");
    let built = builder.build().unwrap();
    let Pipeline::RecordBatch(p) = built else {
        panic!("expected RecordBatch pipeline");
    };
    assert_eq!(Some(select_spec!("col")), p.select);
    let RecordBatchSink::Write {
        output_file_type, ..
    } = &p.sink
    else {
        panic!("expected write sink");
    };
    assert_eq!(FileType::Csv, *output_file_type);
}

#[test]
fn test_pipeline_builder_read_parquet_write_orc() {
    let mut builder = PipelineBuilder::new();
    builder
        .read("input.parquet")
        .select(&["one"])
        .write("output.orc");
    let built = builder.build().unwrap();
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(FileType::Parquet, p.input_file_type);
    assert_eq!(Some(select_spec!("one")), p.select);
    let DataFrameSink::Write {
        output_file_type, ..
    } = &p.sink
    else {
        panic!("expected write sink");
    };
    assert_eq!(FileType::Orc, *output_file_type);
}

#[test]
fn test_record_batch_pipeline_execute_orc_to_csv() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("out.csv");
    let mut builder = PipelineBuilder::new();
    builder
        .read("fixtures/userdata.orc")
        .write(out.to_str().expect("utf8 path"));
    let mut built = builder.build().expect("build pipeline");
    let Pipeline::RecordBatch(ref mut p) = built else {
        panic!("expected RecordBatch pipeline");
    };
    p.execute().expect("execute pipeline");
    assert!(out.is_file());
}

#[test]
fn test_dataframe_pipeline_execute_avro_to_orc() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("out.orc");
    let mut builder = PipelineBuilder::new();
    builder
        .read("fixtures/userdata5.avro")
        .select(&["id", "first_name"])
        .write(out.to_str().expect("utf8 path"));
    let mut built = builder.build().expect("build pipeline");
    let Pipeline::DataFrame(ref mut p) = built else {
        panic!("expected DataFrame pipeline");
    };
    p.execute().expect("execute pipeline");
    assert!(out.is_file());
}

#[test]
fn test_pipeline_builder_read_head_display_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").head(3);
    let built = builder.build().expect("build display pipeline");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(FileType::Parquet, p.input_file_type);
    assert_eq!(p.slice, Some(DisplaySlice::Head(3)));
    assert!(p.select.is_none());
    assert!(matches!(p.sink, DataFrameSink::Display { .. }));
}

#[test]
fn test_pipeline_builder_read_head_display_orc_uses_record_batch_pipeline() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/userdata.orc").head(3);
    let built = builder.build().expect("build display pipeline");
    let Pipeline::RecordBatch(p) = built else {
        panic!("expected RecordBatch pipeline");
    };
    assert_eq!(FileType::Orc, p.input_file_type);
    assert_eq!(p.slice, Some(DisplaySlice::Head(3)));
    assert!(matches!(p.sink, RecordBatchSink::Display { .. }));
}

#[test]
fn test_pipeline_builder_read_sample_display_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").sample(2);
    let built = builder.build().expect("build display pipeline");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(FileType::Parquet, p.input_file_type);
    assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
}

#[test]
fn test_pipeline_builder_read_sample_display_orc_uses_record_batch_pipeline() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/userdata.orc").sample(2);
    let built = builder.build().expect("build display pipeline");
    let Pipeline::RecordBatch(p) = built else {
        panic!("expected RecordBatch pipeline");
    };
    assert_eq!(FileType::Orc, p.input_file_type);
    assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
}

#[test]
fn test_pipeline_builder_read_sample_write_csv() {
    let mut builder = PipelineBuilder::new();
    builder
        .read("fixtures/table.parquet")
        .write("out.csv")
        .sample(2);
    let built = builder
        .build()
        .expect("conversion with sample should build");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(p.slice, Some(DisplaySlice::Sample(2)));
}

#[test]
fn test_pipeline_builder_display_rejects_head_and_sample() {
    use crate::errors::PipelinePlanningError;

    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").head(2).sample(3);
    let err = match builder.build() {
        Ok(_) => panic!("head and sample should fail"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        Error::PipelinePlanningError(PipelinePlanningError::ConflictingOptions(ref s))
            if s == "only one of head(n), tail(n), or sample(n) may be set"
    ));
}

#[test]
fn test_record_batch_display_pipeline_execute_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").head(2);
    let mut built = builder.build().expect("build display pipeline");
    built.execute().expect("execute display pipeline");
}

#[test]
fn test_pipeline_builder_read_tail_display_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").tail(2);
    let built = builder.build().expect("build tail display pipeline");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert_eq!(p.slice, Some(DisplaySlice::Tail(2)));
}

#[test]
fn test_record_batch_display_pipeline_execute_tail_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").tail(2);
    let mut built = builder.build().expect("build tail display pipeline");
    built.execute().expect("execute tail display pipeline");
}

#[test]
fn test_record_batch_display_pipeline_execute_head_orc() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/userdata.orc").head(2);
    let mut built = builder.build().expect("build orc display pipeline");
    let Pipeline::RecordBatch(_) = &built else {
        panic!("expected RecordBatch pipeline");
    };
    built.execute().expect("execute orc display pipeline");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataframe_display_pipeline_execute_sample_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").sample(2);
    let mut built = builder.build().expect("build sample display pipeline");
    built.execute().expect("execute sample display pipeline");
}

#[test]
fn test_record_batch_display_pipeline_execute_sample_orc() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/userdata.orc").sample(2);
    let mut built = builder.build().expect("build orc sample display pipeline");
    let Pipeline::RecordBatch(_) = &built else {
        panic!("expected RecordBatch pipeline");
    };
    built
        .execute()
        .expect("execute orc sample display pipeline");
}

#[test]
fn test_pipeline_builder_read_schema_display_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").schema();
    let built = builder.build().expect("build schema display pipeline");
    let Pipeline::DataFrame(p) = built else {
        panic!("expected DataFrame pipeline");
    };
    assert!(p.slice.is_none());
    assert!(matches!(p.sink, DataFrameSink::Schema { .. }));
}

#[test]
fn test_pipeline_builder_read_row_count_display_orc() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/userdata.orc").row_count();
    let built = builder.build().expect("build row count pipeline");
    let Pipeline::RecordBatch(p) = built else {
        panic!("expected RecordBatch pipeline");
    };
    assert!(matches!(p.sink, RecordBatchSink::Count));
}

#[test]
fn test_display_pipeline_execute_schema_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").schema();
    let mut built = builder.build().expect("build");
    built.execute().expect("execute schema");
}

#[test]
fn test_display_pipeline_execute_row_count_parquet() {
    let mut builder = PipelineBuilder::new();
    builder.read("fixtures/table.parquet").row_count();
    let mut built = builder.build().expect("build");
    built.execute().expect("execute row count");
}

#[test]
fn test_column_spec_resolve() {
    let schema = Schema::new(vec![
        Field::new("one", DataType::Int32, false),
        Field::new("two", DataType::Int32, false),
    ]);
    let spec = ColumnSpec::Exact("one".to_string());
    let name = spec.resolve(&schema).unwrap();
    assert_eq!(name, "one");

    let spec = ColumnSpec::CaseInsensitive("ONE".to_string());
    let name = spec.resolve(&schema).unwrap();
    assert_eq!(name, "one");
}

#[test]
fn test_select_spec_from_cli_args_none() {
    assert!(SelectSpec::from_cli_args(&None).is_none());
}

#[test]
fn test_select_spec_from_cli_args_parsing() {
    assert!(SelectSpec::from_cli_args(&Some(vec![])).is_none());
    let spec = SelectSpec::from_cli_args(&Some(vec!["a".to_string(), "b".to_string()]))
        .expect("non-empty");
    assert_eq!(
        spec.columns,
        vec![
            SelectItem::Column(ColumnSpec::Exact("a".into())),
            SelectItem::Column(ColumnSpec::Exact("b".into())),
        ]
    );
    let spec = SelectSpec::from_cli_args(&Some(vec!["a, b".to_string(), "c".to_string()]))
        .expect("non-empty");
    assert_eq!(
        spec.columns,
        vec![
            SelectItem::Column(ColumnSpec::Exact("a".into())),
            SelectItem::Column(ColumnSpec::Exact("b".into())),
            SelectItem::Column(ColumnSpec::Exact("c".into())),
        ]
    );
    let spec =
        SelectSpec::from_cli_args(&Some(vec![" one ,  two  ".to_string()])).expect("non-empty");
    assert_eq!(
        spec.columns,
        vec![
            SelectItem::Column(ColumnSpec::Exact("one".into())),
            SelectItem::Column(ColumnSpec::Exact("two".into())),
        ]
    );
}

#[test]
fn test_select_spec_from_cli_args_empty_fragments() {
    assert!(SelectSpec::from_cli_args(&Some(vec!["  ,  ".to_string()])).is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_parquet_write_avro_steps() {
    let read_args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
    let read_step = DataframeParquetReader { args: read_args };
    let tempfile = NamedTempFile::with_suffix(".avro").expect("Failed to create temp file");
    let write_args = WriteArgs {
        path: tempfile
            .path()
            .to_str()
            .expect("Failed to get path")
            .to_string(),
        file_type: FileType::Avro,
        sparse: None,
        pretty: None,
    };
    let write_step = DataframeAvroWriter { args: write_args };
    write_step
        .execute(Box::new(read_step))
        .await
        .expect("Failed to write Avro file");
    assert!(tempfile.path().exists());
    let schema = get_schema_fields_avro(tempfile.path().to_str().expect("Failed to get path"))
        .expect("Failed to get schema fields");
    assert_eq!(schema.len(), 6, "Expected 6 columns");
    assert_eq!(schema[0].name, "one", "Expected first column name is 'one'");
    assert_eq!(schema[0].data_type, "Float64", "Expected Float64 data type");
    assert!(schema[0].nullable, "Expected nullable column");
}
