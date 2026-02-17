#![doc = include_str!("../README.md")]

/// The datu crate version
pub const VERSION: &str = clap::crate_version!();

pub mod cli;
pub mod errors;
pub mod pipeline;
pub mod utils;

pub use errors::Error;
pub use utils::FileType;

/// Result type alias for datu operations.
pub type Result<T> = std::result::Result<T, Error>;
