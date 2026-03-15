//! `datu count` - return the number of rows in a Parquet, Avro, or ORC file

use datu::cli::CountArgs;
use datu::pipeline::build_reader;
use datu::resolve_input_file_type;

/// The `datu count` command
pub async fn count(args: CountArgs) -> anyhow::Result<()> {
    let file_type = resolve_input_file_type(args.input, &args.input_path)?;
    let mut reader_step =
        build_reader(&args.input_path, file_type, None, None, args.input_headers)?;

    let reader = reader_step.get()?;
    let mut total: usize = 0;
    for batch in reader {
        let batch = batch.map_err(anyhow::Error::from)?;
        total += batch.num_rows();
    }

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
