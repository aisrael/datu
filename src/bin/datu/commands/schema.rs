//! `datu schema` - display the schema of a Parquet, Avro, or ORC file

use datu::cli::SchemaArgs;
use datu::pipeline::PipelineBuilder;
use datu::resolve_file_type;
use eyre::Result;

/// The `datu schema` command
pub async fn schema(args: SchemaArgs) -> Result<()> {
    resolve_file_type(args.input, &args.input_path).map_err(eyre::Report::from)?;
    let mut builder = PipelineBuilder::new();
    builder
        .read(&args.input_path)
        .input_type(args.input)
        .csv_has_header(args.input_headers)
        .sparse(args.sparse)
        .display_format(args.output)
        .schema();
    let mut built = builder.build().map_err(eyre::Report::from)?;
    built.execute().map_err(eyre::Report::from)
}
