use arrow::record_batch::RecordBatch;
use arrow_json::ArrayWriter;
use saphyr::Yaml;
use saphyr::YamlEmitter;

use crate::Error;
use crate::Result;
use crate::cli::DisplayOutputFormat;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;

fn record_batch_to_yaml_rows(batch: &RecordBatch) -> Vec<Yaml<'static>> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    (0..num_rows)
        .map(|row_idx| {
            let mut map = hashlink::LinkedHashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col_name = field.name().clone();
                let array = batch.column(col_idx);
                let value_str =
                    arrow::util::display::array_value_to_string(array.as_ref(), row_idx)
                        .unwrap_or_else(|_| "-".to_string());
                map.insert(
                    Yaml::scalar_from_string(col_name),
                    Yaml::scalar_from_string(value_str),
                );
            }
            Yaml::Mapping(map)
        })
        .collect()
}

/// Pipeline step that writes record batches to stdout as CSV or JSON.
pub struct DisplayWriterStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub output_format: DisplayOutputFormat,
}

impl Step for DisplayWriterStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = ();

    fn execute(mut self) -> Result<Self::Output> {
        let reader = self.prev.get_record_batch_reader()?;
        match self.output_format {
            DisplayOutputFormat::Csv => {
                let mut writer = arrow::csv::Writer::new(std::io::stdout());
                for batch in reader {
                    let batch = batch.map_err(Error::ArrowError)?;
                    writer.write(&batch).map_err(Error::ArrowError)?;
                }
            }
            DisplayOutputFormat::Json => {
                let batches: Vec<RecordBatch> = reader
                    .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
                    .map_err(Error::ArrowError)?;
                let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
                let mut writer = ArrayWriter::new(std::io::stdout());
                writer
                    .write_batches(&batch_refs)
                    .map_err(Error::ArrowError)?;
                writer.finish().map_err(Error::ArrowError)?;
            }
            DisplayOutputFormat::Yaml => {
                let batches: Vec<RecordBatch> = reader
                    .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
                    .map_err(Error::ArrowError)?;
                let yaml_rows: Vec<Yaml<'static>> = batches
                    .iter()
                    .flat_map(|batch| record_batch_to_yaml_rows(batch))
                    .collect();
                let doc = Yaml::Sequence(yaml_rows);
                let mut out = String::new();
                let mut emitter = YamlEmitter::new(&mut out);
                emitter
                    .dump(&doc)
                    .map_err(|e| Error::GenericError(format!("Failed to emit YAML: {e}")))?;
                print!("{out}");
            }
        }
        Ok(())
    }
}
