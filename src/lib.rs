#![doc = include_str!("../README.md")]

/// The datu crate version
pub const VERSION: &str = clap::crate_version!();

pub mod cli;
pub mod errors;
pub mod pipeline;
pub mod utils;

pub use errors::Error;
pub use utils::FileType;
pub use utils::resolve_input_file_type;

/// Result type alias for datu operations.
pub type Result<T> = std::result::Result<T, Error>;
