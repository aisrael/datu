use datu::DISPLAY_PIPELINE_INPUTS_FOR_CLI;
use datu::cli::HeadsOrTails;
use datu::pipeline::PipelineBuilder;
use datu::pipeline::SelectSpec;
use datu::resolve_file_type;
use eyre::Result;
use eyre::bail;

/// tail command implementation: print the last N lines of an Avro, CSV, JSON, Parquet, or ORC file.
pub async fn tail(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    if !input_file_type.supports_pipeline_display_input() {
        bail!("Only {DISPLAY_PIPELINE_INPUTS_FOR_CLI} are supported for tail");
    }

    let mut builder = PipelineBuilder::new();
    builder
        .read(&args.input_path)
        .input_type(args.input)
        .tail(args.number)
        .csv_has_header(args.input_headers)
        .sparse(args.sparse)
        .display_format(args.output)
        .display_csv_headers(args.output_headers.unwrap_or(true));

    if let Some(spec) = SelectSpec::from_cli_args(&args.select) {
        builder.select_spec(spec);
    }

    let mut built = builder.build().map_err(eyre::Report::from)?;
    built.execute().map_err(eyre::Report::from)
}
