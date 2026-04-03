use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;

pub fn assert_valid_parquet_file(path: &str) {
    let file = std::fs::File::open(path).expect("Failed to open file");
    let reader = SerializedFileReader::new(file)
        .expect("Expected file to be valid Parquet, but reading failed");
    let metadata = reader.metadata();
    assert!(
        !metadata.file_metadata().schema().get_fields().is_empty(),
        "Expected Parquet file to have at least one column"
    );
}
