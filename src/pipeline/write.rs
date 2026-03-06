/// Arguments for writing a file (CSV, Avro, Parquet, ORC, XLSX).
pub struct WriteArgs {
    pub path: String,
}

/// Arguments for writing a JSON file.
pub struct WriteJsonArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
    /// When true, format output with indentation and newlines.
    pub pretty: bool,
}

/// Arguments for writing a YAML file.
pub struct WriteYamlArgs {
    pub path: String,
    /// When true, omit keys with null/missing values. When false, output default values.
    pub sparse: bool,
}
