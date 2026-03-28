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

/// Human-readable list of formats accepted for `head`, `tail`, and `sample` CLI commands.
/// Keep in sync with [`FileType::supports_pipeline_display_input`].
pub const DISPLAY_PIPELINE_INPUTS_FOR_CLI: &str = "Parquet, Avro, CSV, JSON, and ORC";

impl FileType {
    /// Input formats allowed for [`crate::pipeline::PipelineBuilder`] conversion (read + write).
    #[inline]
    pub fn supports_pipeline_conversion_input(self) -> bool {
        matches!(
            self,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json | FileType::Orc
        )
    }

    /// Output formats allowed for [`crate::pipeline::PipelineBuilder`] conversion.
    #[inline]
    pub fn supports_pipeline_conversion_output(self) -> bool {
        matches!(
            self,
            FileType::Parquet
                | FileType::Csv
                | FileType::Json
                | FileType::Orc
                | FileType::Avro
                | FileType::Xlsx
                | FileType::Yaml
        )
    }

    /// Input formats for display pipelines (head / tail / sample to stdout).
    #[inline]
    pub fn supports_pipeline_display_input(self) -> bool {
        matches!(
            self,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc | FileType::Json
        )
    }

    /// Formats read via DataFusion in [`crate::pipeline::read::read_to_dataframe`].
    #[inline]
    pub fn supports_datafusion_file_read(self) -> bool {
        matches!(
            self,
            FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Json
        )
    }
}

/// Resolve the input file type: use the explicit override if provided, otherwise infer from the file path extension.
pub fn resolve_file_type(input_override: Option<FileType>, path: &str) -> crate::Result<FileType> {
    input_override.map_or_else(|| FileType::try_from(path), Ok)
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

    #[test]
    fn supports_pipeline_conversion_input() {
        for t in [
            FileType::Parquet,
            FileType::Avro,
            FileType::Csv,
            FileType::Json,
            FileType::Orc,
        ] {
            assert!(t.supports_pipeline_conversion_input(), "{t}");
        }
        assert!(!FileType::Xlsx.supports_pipeline_conversion_input());
        assert!(!FileType::Yaml.supports_pipeline_conversion_input());
    }

    #[test]
    fn supports_pipeline_conversion_output() {
        for t in [
            FileType::Parquet,
            FileType::Csv,
            FileType::Json,
            FileType::Orc,
            FileType::Avro,
            FileType::Xlsx,
            FileType::Yaml,
        ] {
            assert!(t.supports_pipeline_conversion_output(), "{t}");
        }
    }

    #[test]
    fn supports_pipeline_display_input() {
        for t in [
            FileType::Parquet,
            FileType::Avro,
            FileType::Csv,
            FileType::Orc,
            FileType::Json,
        ] {
            assert!(t.supports_pipeline_display_input(), "{t}");
        }
        assert!(!FileType::Xlsx.supports_pipeline_display_input());
        assert!(!FileType::Yaml.supports_pipeline_display_input());
    }

    #[test]
    fn supports_datafusion_file_read() {
        for t in [
            FileType::Parquet,
            FileType::Avro,
            FileType::Csv,
            FileType::Json,
        ] {
            assert!(t.supports_datafusion_file_read(), "{t}");
        }
        assert!(!FileType::Orc.supports_datafusion_file_read());
        assert!(!FileType::Xlsx.supports_datafusion_file_read());
        assert!(!FileType::Yaml.supports_datafusion_file_read());
    }
}
