use anyhow::Result;
use anyhow::bail;
use datu::FileType;
use datu::cli::HeadsOrTails;
use datu::pipeline::build_reader;
use datu::pipeline::display::apply_select_and_display;
use datu::pipeline::record_batch_filter::parse_select_step;
use datu::resolve_input_file_type;

/// head command implementation: print the first N lines of an Avro, CSV, Parquet, or ORC file.
pub async fn head(args: HeadsOrTails) -> Result<()> {
    let input_file_type = resolve_input_file_type(args.input, &args.input_path)?;
    match input_file_type {
        FileType::Parquet | FileType::Avro | FileType::Csv | FileType::Orc => {}
        _ => bail!("Only Parquet, Avro, CSV, and ORC are supported for head"),
    }
    // Pass offset=0 when limiting so ORC row selection applies (it requires both offset and limit).
    let reader_step = build_reader(
        &args.input_path,
        input_file_type,
        Some(args.number),
        Some(0),
        args.input_headers,
    )?;
    apply_select_and_display(
        reader_step,
        parse_select_step(&args.select),
        args.output,
        args.sparse,
        args.output_headers.unwrap_or(true),
    )
    .await
    .map_err(Into::into)
}
