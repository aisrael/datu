use arrow_avro::reader::ReaderBuilder;

/// Asserts that the Avro file at `path` is valid and was written with `expected_codec`
/// (e.g. "none", "deflate", "snappy"). Absent `avro.codec` metadata is treated as "none".
pub fn assert_avro_codec(path: &str, expected_codec: &str) {
    let file = std::fs::File::open(path).expect("Failed to open file");
    let reader = ReaderBuilder::new()
        .build(std::io::BufReader::new(file))
        .expect("Expected file to be valid Avro, but reading failed");
    let codec = reader
        .avro_header()
        .metadata()
        .find(|(key, _)| *key == b"avro.codec")
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
        .unwrap_or_else(|| "none".to_string());
    let normalized = if codec == "null" { "none" } else { &codec };
    assert_eq!(
        normalized, expected_codec,
        "Expected Avro file {path} to have codec '{expected_codec}', got '{codec}'"
    );
}
