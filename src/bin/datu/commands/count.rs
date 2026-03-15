//! `datu count` - return the number of rows in a Parquet, Avro, CSV, or ORC file

use datu::cli::CountArgs;
use datu::pipeline::count_rows;
use datu::resolve_file_type;

/// The `datu count` command. Uses metadata for Parquet and ORC (no data read);
/// streams batches for Avro and CSV.
pub async fn count(args: CountArgs) -> anyhow::Result<()> {
    let file_type = resolve_file_type(args.input, &args.input_path)?;
    let total = count_rows(&args.input_path, file_type, args.input_headers)?;
    println!("{total}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_parquet() {
        let args = CountArgs {
            input_path: "fixtures/table.parquet".to_string(),
            input: None,
            input_headers: None,
        };
        let result = count(args).await;
        assert!(result.is_ok(), "count failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_count_avro() {
        let args = CountArgs {
            input_path: "fixtures/userdata5.avro".to_string(),
            input: None,
            input_headers: None,
        };
        let result = count(args).await;
        assert!(result.is_ok(), "count failed: {:?}", result.err());
    }
}
