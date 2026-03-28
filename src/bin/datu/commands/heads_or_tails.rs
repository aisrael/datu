//! head, tail, and sample CLI commands share the same pipeline setup.

use datu::DISPLAY_PIPELINE_INPUTS_FOR_CLI;
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
}

/// head, tail, or sample: print or sample rows from supported pipeline input formats.
pub async fn heads_or_tails(args: HeadsOrTails, cmd: HeadsOrTailsCmd) -> Result<()> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    if !input_file_type.supports_pipeline_display_input() {
        bail!(
            "Only {DISPLAY_PIPELINE_INPUTS_FOR_CLI} are supported for {}",
            cmd.name()
        );
    }

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
        .sparse(args.sparse)
        .display_format(args.output)
        .display_csv_headers(args.output_headers.unwrap_or(true));

    if let Some(spec) = SelectSpec::from_cli_args(&args.select) {
        builder.select_spec(spec);
    }

    let mut pipeline = builder.build()?;
    Ok(pipeline.execute()?)
}
