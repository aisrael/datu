//! `datu schema` - display the schema of a Parquet, Avro, or ORC file

use anyhow::Result;
use datu::cli::SchemaArgs;
use datu::pipeline::schema::get_schema_fields;
use datu::pipeline::schema::print_schema_fields;
use datu::resolve_file_type;

/// The `datu schema` command
pub async fn schema(args: SchemaArgs) -> Result<()> {
    let file_type = resolve_file_type(args.input, &args.input_path)?;
    let fields = get_schema_fields(&args.input_path, file_type, args.input_headers)?;
    print_schema_fields(&fields, args.output, args.sparse)
}
