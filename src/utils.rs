use std::fs::File;

use orc_rust::reader::metadata::read_metadata;
use parquet::file::metadata::ParquetMetaDataReader;
use rustc_literal_escaper::unescape_str as unescape_str_raw;

use crate::Error;
use crate::FileType;

/// Unescape a string as if it were a Rust string literal.
/// Returns the unescaped string, or an error if the input contains invalid escape sequences.
pub fn unescape_str(s: &str) -> Result<String, rustc_literal_escaper::EscapeError> {
    let mut result = String::new();
    let mut first_error = None;
    unescape_str_raw(s, |_range, char_result| {
        if first_error.is_some() {
            return;
        }
        match char_result {
            Ok(c) => result.push(c),
            Err(e) => first_error = Some(e),
        }
    });
    first_error.map_or(Ok(result), Err)
}

/// Returns the total number of rows from file metadata for seekable formats (Parquet, ORC).
/// Returns an error for unsupported formats or if metadata cannot be read.
pub fn get_total_rows_result(path: &str, file_type: FileType) -> crate::Result<usize> {
    match file_type {
        FileType::Parquet => {
            let file = File::open(path).map_err(Error::IoError)?;
            let metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&file)
                .map_err(Error::ParquetError)?;
            Ok(metadata.file_metadata().num_rows().max(0) as usize)
        }
        FileType::Orc => {
            let mut file = File::open(path).map_err(Error::IoError)?;
            let metadata = read_metadata(&mut file).map_err(Error::OrcError)?;
            Ok(metadata.number_of_rows() as usize)
        }
        _ => Err(Error::GenericError(format!(
            "get_total_rows_result is only supported for Parquet and ORC files, got: {file_type}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape_str() {
        assert_eq!(unescape_str("\\u{1f4a9}").unwrap(), "💩");
        assert_eq!(unescape_str(r#"\"one:\""#).unwrap(), r#""one:""#);
    }
}
