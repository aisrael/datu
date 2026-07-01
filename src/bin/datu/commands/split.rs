//! `datu split` — split a single input file into multiple output files of at most N rows each.

use std::time::Duration;

use clap::Args;
use datu::FileType;
use datu::pipeline::split_file;
use eyre::Result;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

/// Arguments for the `datu split` command.
#[derive(Args)]
pub struct SplitArgs {
    /// Path to the input file to split
    pub input_path: String,
    /// Optional base path for output partitions (directory, stem, and extension are reused for
    /// every partition; extension is overridden by --output when given). Defaults to the input
    /// path, so partitions are written alongside the input file.
    pub output_path: Option<String>,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub input: Option<FileType>,
    #[arg(
        long,
        short = 'O',
        value_parser = clap::value_parser!(FileType),
        help = "Output file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection."
    )]
    pub output: Option<FileType>,
    #[arg(
        long,
        default_value_t = 100_000,
        help = "Maximum number of rows per output partition."
    )]
    pub split: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "Maximum number of total rows to process across all partitions. Use 0 for unlimited."
    )]
    pub limit: usize,
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "For JSON/YAML: omit keys with null/missing values. Default: true. Use --sparse=false to include default values (e.g. empty string)."
    )]
    pub sparse: bool,
    #[arg(
        long,
        help = "When splitting to JSON, format output with indentation and newlines. Ignored for other output formats."
    )]
    pub json_pretty: bool,
    #[arg(
        long,
        value_parser = clap::value_parser!(bool),
        num_args = 0..=1,
        default_missing_value = "true",
        help = "For CSV input: whether the first row is a header. Default: true when omitted. Use --input-headers=false for headerless CSV."
    )]
    pub input_headers: Option<bool>,
}

/// Creates a spinner progress bar for the split command; the message is updated with running
/// partition/row counts as each partition is written.
fn create_progress_bar(input_path: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner:.cyan} {msg}").expect("valid template"));
    pb.set_message(format!("Splitting {input_path}..."));
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Splits a single input file into partitions of at most `--split` rows each; input/output
/// formats are inferred from extensions unless overridden, just like `convert` and `concat`.
pub async fn split(args: SplitArgs) -> Result<()> {
    let output_base = args
        .output_path
        .clone()
        .unwrap_or_else(|| args.input_path.clone());

    let progress = create_progress_bar(&args.input_path);

    let result = split_file(
        &args.input_path,
        args.input,
        &output_base,
        args.output,
        args.input_headers,
        args.split,
        args.limit,
        args.sparse,
        args.json_pretty,
        Some(progress.clone()),
    )
    .await;

    match result {
        Ok(outcome) => {
            progress.finish_and_clear();
            let suffix = if outcome.partitions_written == 1 {
                ""
            } else {
                "s"
            };
            eprintln!(
                "Split {} into {} file{suffix} ({} rows)",
                args.input_path, outcome.partitions_written, outcome.rows_written
            );
            Ok(())
        }
        Err(e) => {
            progress.abandon();
            eprintln!("Failed to split {}", args.input_path);
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split_args_for_tests(
        input_path: &str,
        output_path: Option<&str>,
        split: usize,
    ) -> SplitArgs {
        SplitArgs {
            input_path: input_path.to_string(),
            output_path: output_path.map(|s| s.to_string()),
            input: None,
            output: None,
            split,
            limit: 0,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_parquet_to_partitions() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.parquet");
        let args = split_args_for_tests(
            "fixtures/table.parquet",
            Some(output_path.to_str().expect("valid utf-8 path")),
            2,
        );

        let result = split(args).await;
        assert!(result.is_ok(), "split failed: {:?}", result.err());
        assert!(temp_dir.path().join("out.part00001.parquet").exists());
        assert!(temp_dir.path().join("out.part00002.parquet").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_avro_to_csv() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.csv");
        let mut args = split_args_for_tests(
            "fixtures/concat_part1.avro",
            Some(output_path.to_str().expect("valid utf-8 path")),
            2,
        );
        args.output = Some(FileType::Csv);

        let result = split(args).await;
        assert!(result.is_ok(), "split failed: {:?}", result.err());
        assert!(temp_dir.path().join("out.part00001.csv").exists());
        assert!(temp_dir.path().join("out.part00002.csv").exists());
        assert!(temp_dir.path().join("out.part00003.csv").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_split_rejects_zero_split_size() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("out.parquet");
        let args = split_args_for_tests(
            "fixtures/table.parquet",
            Some(output_path.to_str().expect("valid utf-8 path")),
            0,
        );

        let result = split(args).await;
        assert!(result.is_err(), "split with --split 0 should fail");
    }
}
