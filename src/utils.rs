use std::fs::File;
use std::path::Path;
use std::str::FromStr;

use orc_rust::reader::metadata::read_metadata;
use parquet::file::metadata::ParquetMetaDataReader;
use rustc_literal_escaper::unescape_str as unescape_str_raw;

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

/// Parse column names from `select` by splitting each string at commas, trimming and
/// discarding empty parts. E.g. `["a, b", "c"]` becomes `["a", "b", "c"]`.
pub fn parse_select_columns(select: &[String]) -> Vec<String> {
    let mut columns = Vec::with_capacity(select.len());
    for s in select {
        columns.extend(s.split(',').filter_map(|c| {
            let c = c.trim();
            if !c.is_empty() {
                Some(c.to_string())
            } else {
                None
            }
        }));
    }
    columns
}

/// A supported input or output file type
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileType {
    Avro,
    Csv,
    Json,
    Orc,
    Parquet,
    Xlsx,
    Yaml,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Avro => write!(f, "avro"),
            FileType::Csv => write!(f, "csv"),
            FileType::Json => write!(f, "json"),
            FileType::Orc => write!(f, "orc"),
            FileType::Parquet => write!(f, "parquet"),
            FileType::Xlsx => write!(f, "xlsx"),
            FileType::Yaml => write!(f, "yaml"),
        }
    }
}

impl FromStr for FileType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(FileType::Avro),
            "csv" => Ok(FileType::Csv),
            "json" => Ok(FileType::Json),
            "orc" => Ok(FileType::Orc),
            "parq" | "parquet" => Ok(FileType::Parquet),
            "xlsx" => Ok(FileType::Xlsx),
            "yaml" | "yml" => Ok(FileType::Yaml),
            _ => Err(format!(
                "unknown file type '{s}', expected one of: avro, csv, json, orc, parquet, xlsx, yaml"
            )),
        }
    }
}

/// Try to determine the FileType from a filename
impl TryFrom<&str> for FileType {
    type Error = crate::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let path = Path::new(s);

        if let Some(extension) = path.extension()
            && let Some(s) = extension.to_str()
        {
            let file_type = match s.to_lowercase().as_str() {
                "json" => FileType::Json,
                "csv" => FileType::Csv,
                "parq" | "parquet" => FileType::Parquet,
                "orc" => FileType::Orc,
                "avro" => FileType::Avro,
                "xlsx" => FileType::Xlsx,
                "yaml" | "yml" => FileType::Yaml,
                _ => return Err(crate::Error::UnknownFileType(s.to_owned())),
            };
            return Ok(file_type);
        };

        Err(crate::Error::UnknownFileType(s.to_owned()))
    }
}

/// Resolve the input file type: use the explicit override if provided, otherwise infer from the file path extension.
pub fn resolve_input_file_type(
    input_override: Option<FileType>,
    path: &str,
) -> crate::Result<FileType> {
    match input_override {
        Some(ft) => Ok(ft),
        None => FileType::try_from(path),
    }
}

/// Returns the total number of rows from file metadata for seekable formats (Parquet, ORC).
/// Returns an error for unsupported formats or if metadata cannot be read.
pub fn get_total_rows_result(path: &str, file_type: FileType) -> crate::Result<usize> {
    match file_type {
        FileType::Parquet => {
            let file = File::open(path).map_err(crate::Error::IoError)?;
            let metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&file)
                .map_err(crate::Error::ParquetError)?;
            Ok(metadata.file_metadata().num_rows().max(0) as usize)
        }
        FileType::Orc => {
            let mut file = File::open(path).map_err(crate::Error::IoError)?;
            let metadata = read_metadata(&mut file).map_err(crate::Error::OrcError)?;
            Ok(metadata.number_of_rows() as usize)
        }
        _ => Err(crate::Error::GenericError(format!(
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

    #[test]
    fn test_parse_select_columns() {
        assert_eq!(parse_select_columns(&[]), Vec::<String>::new());
        assert_eq!(
            parse_select_columns(&["a".to_string(), "b".to_string()]),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            parse_select_columns(&["a, b".to_string(), "c".to_string()]),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert_eq!(
            parse_select_columns(&[" one ,  two  ".to_string()]),
            vec!["one".to_string(), "two".to_string()]
        );
    }

    #[test]
    fn test_valid_extensions() {
        assert_eq!(FileType::try_from("test.csv").unwrap(), FileType::Csv);
        assert_eq!(FileType::try_from("data.json").unwrap(), FileType::Json);
        assert_eq!(FileType::try_from("file.parq").unwrap(), FileType::Parquet);
        assert_eq!(FileType::try_from("data.orc").unwrap(), FileType::Orc);
        assert_eq!(FileType::try_from("schema.avro").unwrap(), FileType::Avro);
        assert_eq!(FileType::try_from("data.xlsx").unwrap(), FileType::Xlsx);
        assert_eq!(FileType::try_from("data.yaml").unwrap(), FileType::Yaml);
        assert_eq!(FileType::try_from("data.yml").unwrap(), FileType::Yaml);
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(FileType::try_from("test.CSV").unwrap(), FileType::Csv);
        assert_eq!(FileType::try_from("data.Json").unwrap(), FileType::Json);
        assert_eq!(FileType::try_from("file.PARQ").unwrap(), FileType::Parquet);
        assert_eq!(FileType::try_from("data.ORC").unwrap(), FileType::Orc);
        assert_eq!(FileType::try_from("schema.Avro").unwrap(), FileType::Avro);
        assert_eq!(FileType::try_from("report.XLSX").unwrap(), FileType::Xlsx);
        assert_eq!(FileType::try_from("config.YAML").unwrap(), FileType::Yaml);
        assert_eq!(FileType::try_from("config.YML").unwrap(), FileType::Yaml);
    }

    #[test]
    fn test_unknown_extension() {
        let result = FileType::try_from("image.png");
        assert!(matches!(result, Err(crate::Error::UnknownFileType(s)) if s == "png"));
    }

    #[test]
    fn test_no_extension() {
        let result = FileType::try_from("README");
        assert!(matches!(result, Err(crate::Error::UnknownFileType(s)) if s == "README"));
    }
}
