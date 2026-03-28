#![doc = include_str!("../README.md")]

/// The datu crate version
pub const VERSION: &str = clap::crate_version!();

pub mod cli;
pub mod errors;
pub mod file_type;
pub mod pipeline;
pub mod utils;

/// Primary error type for datu operations.
pub use errors::Error;
/// Human-readable list of formats accepted for `head`, `tail`, and `sample` CLI commands.
pub use file_type::DISPLAY_PIPELINE_INPUTS_FOR_CLI;
/// Supported input/output formats and extension-based detection.
pub use file_type::FileType;
/// Resolves an explicit file type or infers it from a path.
pub use file_type::resolve_file_type;
/// Total row count from file metadata (Parquet and ORC only).
pub use utils::get_total_rows_result;

/// Result type alias for datu operations.
pub type Result<T> = std::result::Result<T, Error>;
