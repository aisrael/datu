//! `datu concat` — concatenate two or more input files into a single output file.

use std::time::Duration;

use clap::Args;
use datu::FileType;
use datu::pipeline::concat_files;
use eyre::Context;
use eyre::Result;
use eyre::bail;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

/// Arguments for the `datu concat` command.
#[derive(Args)]
pub struct ConcatArgs {
    /// Input file paths or globs, followed by the output file path (at least 2 total). All
    /// arguments except the last are inputs; the last is the concatenated output file.
    #[arg(required = true, num_args = 2.., value_name = "INPUT_OR_OUTPUT")]
    pub paths: Vec<String>,
    #[arg(
        long,
        short = 'I',
        value_parser = clap::value_parser!(FileType),
        help = "Input file type (avro, csv, json, orc, parquet, xlsx, yaml). Overrides extension-based detection for every input."
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
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "For JSON/YAML: omit keys with null/missing values. Default: true. Use --sparse=false to include default values (e.g. empty string)."
    )]
    pub sparse: bool,
    #[arg(
        long,
        help = "When concatenating to JSON, format output with indentation and newlines. Ignored for other output formats."
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

/// Splits CLI positional arguments into input patterns (all but the last) and the output path
/// (the last). Requires at least one input plus the output.
fn split_inputs_and_output(paths: &[String]) -> Result<(&[String], &str)> {
    if paths.len() < 2 {
        bail!("concat requires at least two paths: one or more inputs, plus an output file");
    }
    let (inputs, output) = paths.split_at(paths.len() - 1);
    Ok((inputs, output[0].as_str()))
}

/// Whether `s` contains shell glob metacharacters supported by the `glob` crate.
fn is_glob_pattern(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

/// Expands glob patterns in CLI order into a flat, deterministic list of input file paths.
/// Non-glob arguments are kept as literal paths (existence is checked later, at read time).
/// Matches for each individual glob pattern are sorted lexicographically.
fn expand_input_paths(patterns: &[String]) -> Result<Vec<String>> {
    let mut expanded = Vec::new();
    for pattern in patterns {
        if !is_glob_pattern(pattern) {
            expanded.push(pattern.clone());
            continue;
        }

        let mut matches: Vec<String> = glob::glob(pattern)
            .with_context(|| format!("invalid glob pattern: {pattern}"))?
            .map(|entry| {
                let path =
                    entry.with_context(|| format!("failed to read glob match for: {pattern}"))?;
                path.into_os_string()
                    .into_string()
                    .map_err(|raw| eyre::eyre!("glob match is not valid UTF-8: {raw:?}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if matches.is_empty() {
            bail!("glob pattern matched no files: {pattern}");
        }
        matches.sort();
        expanded.extend(matches);
    }
    Ok(expanded)
}

/// Creates a spinner progress bar for the concat command (row totals across mixed input formats
/// and globs are not known up front, so this mirrors `convert`'s indeterminate fallback).
fn create_progress_bar(input_count: usize, output_path: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner:.cyan} {msg}").expect("valid template"));
    let suffix = if input_count == 1 { "" } else { "s" };
    pb.set_message(format!(
        "Concatenating {input_count} file{suffix} into {output_path}..."
    ));
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Concatenates one or more input files (or globs) into a single output file; input/output
/// formats are inferred from extensions unless overridden, just like `convert`.
pub async fn concat(args: ConcatArgs) -> Result<()> {
    let (input_patterns, output_path) = split_inputs_and_output(&args.paths)?;
    let input_paths = expand_input_paths(input_patterns)?;
    let output_path = output_path.to_string();

    let progress = create_progress_bar(input_paths.len(), &output_path);

    let result = concat_files(
        &input_paths,
        args.input,
        &output_path,
        args.output,
        args.input_headers,
        args.sparse,
        args.json_pretty,
        Some(progress.clone()),
    )
    .await;

    match result {
        Ok(()) => {
            progress.finish_and_clear();
            let suffix = if input_paths.len() == 1 { "" } else { "s" };
            eprintln!(
                "Concatenated {} file{suffix} into {output_path}",
                input_paths.len()
            );
            Ok(())
        }
        Err(e) => {
            progress.abandon();
            eprintln!("Failed to concatenate into {output_path}");
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_inputs_and_output_requires_two_paths() {
        let paths = ["only_one.avro".to_string()];
        let result = split_inputs_and_output(&paths);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_inputs_and_output() {
        let paths = vec![
            "a.avro".to_string(),
            "b.avro".to_string(),
            "out.parquet".to_string(),
        ];
        let (inputs, output) = split_inputs_and_output(&paths).expect("should split");
        assert_eq!(inputs, ["a.avro".to_string(), "b.avro".to_string()]);
        assert_eq!(output, "out.parquet");
    }

    #[test]
    fn test_is_glob_pattern() {
        assert!(is_glob_pattern("part*.avro"));
        assert!(is_glob_pattern("part?.avro"));
        assert!(is_glob_pattern("part[01].avro"));
        assert!(!is_glob_pattern("part0.avro"));
    }

    #[test]
    fn test_expand_input_paths_literal_paths_kept_in_order() {
        let patterns = vec![
            "fixtures/file2.avro".to_string(),
            "fixtures/file1.avro".to_string(),
        ];
        let expanded = expand_input_paths(&patterns).expect("should expand");
        assert_eq!(
            expanded,
            vec![
                "fixtures/file2.avro".to_string(),
                "fixtures/file1.avro".to_string()
            ]
        );
    }

    #[test]
    fn test_expand_input_paths_glob_matches_sorted() {
        let patterns = vec!["fixtures/file*.avro".to_string()];
        let expanded = expand_input_paths(&patterns).expect("should expand");
        assert_eq!(
            expanded,
            vec![
                "fixtures/file1.avro".to_string(),
                "fixtures/file2.avro".to_string()
            ]
        );
    }

    #[test]
    fn test_expand_input_paths_glob_no_matches_errors() {
        let patterns = vec!["fixtures/does_not_exist_*.avro".to_string()];
        let result = expand_input_paths(&patterns);
        assert!(result.is_err(), "expected error for unmatched glob");
    }

    #[test]
    fn test_expand_input_paths_mixed_literal_and_glob() {
        let patterns = vec![
            "fixtures/table.parquet".to_string(),
            "fixtures/file*.avro".to_string(),
        ];
        let expanded = expand_input_paths(&patterns).expect("should expand");
        assert_eq!(
            expanded,
            vec![
                "fixtures/table.parquet".to_string(),
                "fixtures/file1.avro".to_string(),
                "fixtures/file2.avro".to_string()
            ]
        );
    }

    fn concat_args_for_tests(paths: Vec<String>) -> ConcatArgs {
        ConcatArgs {
            paths,
            input: None,
            output: None,
            sparse: true,
            json_pretty: false,
            input_headers: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_two_avro_files_to_parquet() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("combined.parquet");
        let args = concat_args_for_tests(vec![
            "fixtures/concat_part1.avro".to_string(),
            "fixtures/concat_part2.avro".to_string(),
            output_path.to_str().expect("valid utf-8 path").to_string(),
        ]);

        let result = concat(args).await;
        assert!(result.is_ok(), "concat failed: {:?}", result.err());
        assert!(output_path.exists(), "output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_glob_input_to_parquet() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("combined.parquet");
        let args = concat_args_for_tests(vec![
            "fixtures/concat_part*.avro".to_string(),
            output_path.to_str().expect("valid utf-8 path").to_string(),
        ]);

        let result = concat(args).await;
        assert!(result.is_ok(), "concat failed: {:?}", result.err());
        assert!(output_path.exists(), "output file was not created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_requires_at_least_two_paths() {
        let args = concat_args_for_tests(vec!["fixtures/concat_part1.avro".to_string()]);
        let result = concat(args).await;
        assert!(result.is_err(), "concat with a single path should fail");
    }
}
