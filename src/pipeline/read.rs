/// Arguments for reading a file (Avro, CSV, Parquet, ORC).
pub struct ReadArgs {
    pub path: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    pub csv_has_header: Option<bool>,
}
