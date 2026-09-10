use parquet::basic::Compression;
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

/// Asserts that the Parquet file at `path` was written with `expected_codec` (e.g. "none",
/// "snappy", "gzip", "zstd", "brotli", "lz4", "lz4_raw"). Only the codec identifier is checked —
/// Parquet's on-disk column metadata does not preserve the write-time compression level.
///
/// Only used by the `cli` Cucumber binary; the `repl` binary compiles this same file via its own
/// `#[path]` module declaration but doesn't exercise Parquet compression.
#[allow(dead_code)]
pub fn assert_parquet_codec(path: &str, expected_codec: &str) {
    let file = std::fs::File::open(path).expect("Failed to open file");
    let reader = SerializedFileReader::new(file)
        .expect("Expected file to be valid Parquet, but reading failed");
    let metadata = reader.metadata();
    let compression = metadata.row_group(0).column(0).compression();
    let codec_name = match compression {
        Compression::UNCOMPRESSED => "none",
        Compression::SNAPPY => "snappy",
        Compression::GZIP(_) => "gzip",
        Compression::LZO => "lzo",
        Compression::BROTLI(_) => "brotli",
        Compression::LZ4 => "lz4",
        Compression::ZSTD(_) => "zstd",
        Compression::LZ4_RAW => "lz4_raw",
    };
    assert_eq!(
        codec_name, expected_codec,
        "Expected Parquet file {path} to have codec '{expected_codec}', got '{codec_name}'"
    );
}
