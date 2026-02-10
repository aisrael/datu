use std::path::Path;

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
    Parquet,
    Xlsx,
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
                "avro" => FileType::Avro,
                "xlsx" => FileType::Xlsx,
                _ => return Err(crate::Error::UnknownFileType(s.to_owned())),
            };
            return Ok(file_type);
        };

        Err(crate::Error::UnknownFileType(s.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(FileType::try_from("schema.avro").unwrap(), FileType::Avro);
        assert_eq!(FileType::try_from("data.xlsx").unwrap(), FileType::Xlsx);
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(FileType::try_from("test.CSV").unwrap(), FileType::Csv);
        assert_eq!(FileType::try_from("data.Json").unwrap(), FileType::Json);
        assert_eq!(FileType::try_from("file.PARQ").unwrap(), FileType::Parquet);
        assert_eq!(FileType::try_from("schema.Avro").unwrap(), FileType::Avro);
        assert_eq!(FileType::try_from("report.XLSX").unwrap(), FileType::Xlsx);
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
