//! head, tail, and sample CLI commands share the same pipeline setup.

use datu::DISPLAY_PIPELINE_INPUTS_FOR_CLI;
use datu::cli::DisplayOutputFormat;
use datu::cli::HeadsOrTails;
use datu::pipeline::PipelineBuilder;
use datu::pipeline::SelectSpec;
use datu::resolve_file_type;
use eyre::Result;
use eyre::bail;

/// Which slice-of-rows command to run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadsOrTailsCmd {
    Head,
    Tail,
    Sample,
}

impl HeadsOrTailsCmd {
    fn name(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Tail => "tail",
            Self::Sample => "sample",
        }
    }

    /// Past-tense verb used in the success message when writing to a file.
    fn write_verb(self) -> &'static str {
        match self {
            Self::Head => "Wrote head of",
            Self::Tail => "Wrote tail of",
            Self::Sample => "Sampled",
        }
    }
}

/// head, tail, or sample: print or sample rows from supported pipeline input formats.
pub async fn heads_or_tails(args: HeadsOrTails, cmd: HeadsOrTailsCmd) -> Result<()> {
    let mut builder = PipelineBuilder::new();
    builder.read(&args.input_path).input_type(args.input);
    match cmd {
        HeadsOrTailsCmd::Head => {
            builder.head(args.number);
        }
        HeadsOrTailsCmd::Tail => {
            builder.tail(args.number);
        }
        HeadsOrTailsCmd::Sample => {
            builder.sample(args.number);
        }
    }
    builder
        .csv_has_header(args.input_headers)
        .sparse(args.sparse);

    if let Some(spec) = SelectSpec::from_cli_args(&args.select) {
        builder.select_spec(spec);
    }

    if let Some(output_path) = &args.output_path {
        // Writing to a file: the format comes from the OUTPUT extension (or -O), so the
        // stdout-only display flags would be silently ignored. Reject them explicitly.
        if args.output != DisplayOutputFormat::Csv {
            bail!(
                "--output (stdout display format) cannot be combined with an output file; the file format is determined by the OUTPUT extension or -O/--output-type"
            );
        }
        if args.output_headers == Some(false) {
            bail!(
                "--output-headers only applies when printing to stdout, not when writing to a file"
            );
        }

        builder
            .write(output_path)
            .output_type(args.output_type)
            .json_pretty(args.json_pretty);

        let mut pipeline = builder.build()?;
        pipeline.execute()?;
        eprintln!(
            "{verb} {input} to {output}",
            verb = cmd.write_verb(),
            input = args.input_path,
            output = output_path
        );
        return Ok(());
    }

    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    if !input_file_type.supports_pipeline_display_input() {
        bail!(
            "Only {DISPLAY_PIPELINE_INPUTS_FOR_CLI} are supported for {}",
            cmd.name()
        );
    }

    builder
        .display_format(args.output)
        .display_csv_headers(args.output_headers.unwrap_or(true));

    let mut pipeline = builder.build()?;
    Ok(pipeline.execute()?)
}
