//! `datu count` - return the number of rows in a Parquet, Avro, CSV, or ORC file

use datu::cli::CountArgs;
use datu::pipeline::PipelineBuilder;
use datu::resolve_file_type;
use eyre::Result;

/// The `datu count` command. Uses metadata for Parquet and ORC (no data read);
/// streams batches for Avro and CSV.
pub async fn count(args: CountArgs) -> Result<()> {
    resolve_file_type(args.input, &args.input_path)?;
    let mut builder = PipelineBuilder::new();
    builder
        .read(&args.input_path)
        .input_type(args.input)
        .csv_has_header(args.input_headers)
        .row_count();
    let mut pipeline = builder.build()?;
    Ok(pipeline.execute()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count_parquet() {
        let args = CountArgs {
            input_path: "fixtures/table.parquet".to_string(),
            input: None,
            input_headers: None,
        };
        let result = count(args).await;
        assert!(result.is_ok(), "count failed: {:?}", result.err());
    }

    #[tokio::test(flavor = "multi_thread")]
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
