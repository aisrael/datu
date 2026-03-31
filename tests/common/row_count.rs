use std::io::BufReader;

use arrow_avro::reader::ReaderBuilder;
use datu::FileType;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;

pub fn get_row_count(path: &str) -> usize {
    let file_type = FileType::try_from(path)
        .unwrap_or_else(|e| panic!("Cannot determine file type for {path}: {e}"));
    match file_type {
        FileType::Json => {
            let content = std::fs::read_to_string(path).expect("Failed to read file");
            let value: serde_json::Value =
                serde_json::from_str(content.trim()).expect("Failed to parse JSON");
            value
                .as_array()
                .expect("Expected JSON to be an array")
                .len()
        }
        FileType::Parquet => {
            let file = std::fs::File::open(path).expect("Failed to open file");
            let reader = SerializedFileReader::new(file).expect("Failed to read Parquet file");
            reader
                .metadata()
                .row_groups()
                .iter()
                .map(|rg| rg.num_rows())
                .sum::<i64>() as usize
        }
        FileType::Avro => {
            let file = std::fs::File::open(path).expect("Failed to open file");
            let reader = ReaderBuilder::new()
                .build(BufReader::new(file))
                .expect("Failed to read Avro file");
            reader
                .map(|batch| batch.expect("Failed to read Avro batch").num_rows())
                .sum()
        }
        FileType::Orc => {
            let file = std::fs::File::open(path).expect("Failed to open file");
            let builder = ArrowReaderBuilder::try_new(file).expect("Failed to read ORC file");
            let reader = builder.build();
            reader
                .map(|batch| batch.expect("Failed to read ORC batch").num_rows())
                .sum()
        }
        other => panic!("Unsupported format for record counting: {other:?}"),
    }
}
