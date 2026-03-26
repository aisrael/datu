use std::path::Path;
use std::str::FromStr;

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
pub fn resolve_file_type(input_override: Option<FileType>, path: &str) -> crate::Result<FileType> {
    match input_override {
        Some(ft) => Ok(ft),
        None => FileType::try_from(path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
