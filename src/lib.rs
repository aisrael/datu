#![doc = include_str!("../README.md")]

/// The datu crate version
pub const VERSION: &str = clap::crate_version!();

pub mod cli;
pub mod errors;
pub mod file_type;
pub mod pipeline;
pub mod utils;

pub use errors::Error;
pub use file_type::FileType;
pub use file_type::resolve_file_type;
pub use utils::get_total_rows_result;

/// Result type alias for datu operations.
pub type Result<T> = std::result::Result<T, Error>;
