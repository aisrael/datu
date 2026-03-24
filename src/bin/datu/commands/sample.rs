use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::Pipeline;
use datu::pipeline::PipelineBuilder;
use datu::pipeline::SelectSpec;
use datu::resolve_file_type;
use eyre::Result;
use eyre::bail;

/// sample command implementation: print N random rows from an Avro, CSV, JSON, Parquet, or ORC file.
pub async fn sample(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_file_type(args.input, &args.input_path)?;
    match input_file_type {
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc | FileType::Json => {}
        _ => bail!("Only Parquet, Avro, CSV, JSON, and ORC are supported for sample"),
    }

    let mut builder = PipelineBuilder::new();
    builder
        .read(&args.input_path)
        .input_type(args.input)
        .sample(args.number)
        .csv_has_header(args.input_headers)
        .sparse(args.sparse)
        .display_format(args.output)
        .display_csv_headers(args.output_headers.unwrap_or(true));

    if let Some(spec) = SelectSpec::from_cli_args(&args.select) {
        builder.select_spec(spec);
    }

    let mut built = builder.build().map_err(eyre::Report::from)?;
    match &mut built {
        Pipeline::Display(pipeline) => pipeline.execute().map_err(eyre::Report::from),
        Pipeline::Conversion(_) => {
            bail!("internal error: expected display pipeline for sample");
        }
    }
}
