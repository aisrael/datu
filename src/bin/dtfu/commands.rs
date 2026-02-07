use anyhow::Result;
use anyhow::bail;
use dtfu::FileType;
use dtfu::cli::ConvertArgs;
use dtfu::pipeline::RecordBatchReaderSource;
use dtfu::pipeline::Step;
use dtfu::pipeline::csv::WriteCsvArgs;
use dtfu::pipeline::csv::WriteCsvStep;
use dtfu::pipeline::parquet::ReadParquetArgs;
use dtfu::pipeline::parquet::ReadParquetStep;

/// convert command implementation
pub fn convert(args: ConvertArgs) -> anyhow::Result<()> {
    println!("Converting {} to {}", args.input, args.output);

    let input_file_type: FileType = args.input.as_str().try_into()?;
    let output_file_type: FileType = args.output.as_str().try_into()?;

    let read_step = get_reader_step(input_file_type, &args)?;
    let write_step = get_writer_step(output_file_type, &args)?;

    write_step.step(read_step)?;

    Ok(())
}

pub trait RecordBatchReaderSink {
    fn step(
        &self,
        input: Box<dyn RecordBatchReaderSource>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>>;
}

impl<T> RecordBatchReaderSink for T
where
    T: Step<Input = Box<dyn RecordBatchReaderSource>> + Send + Sync + 'static,
    T::Output: std::any::Any + Send + Sync + 'static,
{
    fn step(
        &self,
        input: Box<dyn RecordBatchReaderSource>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>> {
        let output = self.execute(input)?;
        Ok(Box::new(output))
    }
}

fn get_reader_step(
    input_file_type: FileType,
    args: &ConvertArgs,
) -> Result<Box<dyn RecordBatchReaderSource + 'static>> {
    match input_file_type {
        FileType::Parquet => {
            let reader = ReadParquetStep {
                args: ReadParquetArgs {
                    path: args.input.clone(),
                    limit: None,
                },
            };
            Ok(Box::new(reader))
        }
        _ => bail!("Input file type must be Parquet"),
    }
}

fn get_writer_step(
    output_file_type: FileType,
    args: &ConvertArgs,
) -> Result<Box<dyn RecordBatchReaderSink + Send + Sync + 'static>> {
    match output_file_type {
        FileType::Csv => {
            let writer = Box::new(WriteCsvStep {
                args: WriteCsvArgs {
                    path: args.output.clone(),
                },
            });
            Ok(writer)
        }
        _ => bail!("Output file type must be CSV"),
    }
}
