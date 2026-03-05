use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::Error;
use crate::FileType;
use crate::pipeline::Source;
use crate::pipeline::Step;
use crate::pipeline::VecRecordBatchReader;
use crate::pipeline::avro;
use crate::pipeline::display;
use crate::pipeline::orc;
use crate::pipeline::parquet;
use crate::pipeline::xlsx;
use crate::utils::parse_select_columns;

/// A source that yields a DataFusion DataFrame, implementing `Source<DataFrame>`.
#[derive(Debug)]
pub struct DataFrameSource {
    df: Option<datafusion::dataframe::DataFrame>,
}

impl DataFrameSource {
    pub fn new(df: datafusion::dataframe::DataFrame) -> Self {
        Self { df: Some(df) }
    }
}

impl Source<datafusion::dataframe::DataFrame> for DataFrameSource {
    fn get(&mut self) -> crate::Result<Box<datafusion::dataframe::DataFrame>> {
        let df = self
            .df
            .take()
            .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string()))?;
        Ok(Box::new(df))
    }
}

/// A step that writes a DataFusion DataFrame to an output file.
pub struct DataFrameWriter {
    output_path: String,
    output_file_type: FileType,
    sparse: bool,
    json_pretty: bool,
}

impl DataFrameWriter {
    pub fn new<S: Into<String>>(
        output_path: S,
        output_file_type: FileType,
        sparse: bool,
        json_pretty: bool,
    ) -> Self {
        Self {
            output_path: output_path.into(),
            output_file_type,
            sparse,
            json_pretty,
        }
    }
}

#[async_trait(?Send)]
impl Step for DataFrameWriter {
    type Input = DataFrameSource;
    type Output = ();

    async fn execute(self, mut input: Self::Input) -> crate::Result<Self::Output> {
        if self.output_file_type != FileType::Json && self.json_pretty {
            eprintln!("Warning: --json-pretty is only supported when converting to JSON");
        }

        let df = input.get()?;

        let handle = tokio::runtime::Handle::current();
        let batches = tokio::task::block_in_place(|| handle.block_on(df.collect()))
            .map_err(|e| Error::GenericError(e.to_string()))?;

        let mut reader = VecRecordBatchReader::new(batches);
        let output_path = &self.output_path;

        match self.output_file_type {
            FileType::Parquet => {
                parquet::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Csv => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                let mut writer = arrow::csv::Writer::new(file);
                for batch in &mut reader {
                    let batch = batch.map_err(Error::ArrowError)?;
                    writer.write(&batch).map_err(Error::ArrowError)?;
                }
            }
            FileType::Json => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                if self.json_pretty {
                    display::write_record_batches_as_json_pretty(&mut reader, file, self.sparse)?;
                } else {
                    display::write_record_batches_as_json(&mut reader, file, self.sparse)?;
                }
            }
            FileType::Yaml => {
                let file = std::fs::File::create(output_path).map_err(Error::IoError)?;
                display::write_record_batches_as_yaml(&mut reader, file, self.sparse)?;
            }
            FileType::Avro => {
                avro::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Orc => {
                orc::write_record_batches(output_path, &mut reader)?;
            }
            FileType::Xlsx => {
                xlsx::write_record_batches(output_path, &mut reader)?;
            }
        }

        Ok(())
    }
}

/// A step that reads an input file into a DataFusion DataFrame with optional column selection and limit.
pub struct DataFrameReader {
    input_path: String,
    input_file_type: FileType,
    select: Option<Vec<String>>,
    limit: Option<usize>,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    csv_has_header: Option<bool>,
}

impl DataFrameReader {
    pub fn new(
        input_path: &str,
        input_file_type: FileType,
        select: Option<Vec<String>>,
        limit: Option<usize>,
        csv_has_header: Option<bool>,
    ) -> Self {
        Self {
            input_path: input_path.to_string(),
            input_file_type,
            select,
            limit,
            csv_has_header,
        }
    }

    async fn read(&self) -> crate::Result<DataFrameSource> {
        let ctx = SessionContext::new();

        let mut df = match self.input_file_type {
            FileType::Parquet => ctx
                .read_parquet(&self.input_path, ParquetReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Avro => ctx
                .read_avro(&self.input_path, AvroReadOptions::default())
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?,
            FileType::Csv => {
                let has_header = self.csv_has_header.unwrap_or(true);
                ctx.read_csv(
                    &self.input_path,
                    CsvReadOptions::new().has_header(has_header),
                )
                .await
                .map_err(|e| Error::GenericError(e.to_string()))?
            }
            FileType::Orc => {
                let batches = read_orc_to_batches(&self.input_path)?;
                if batches.is_empty() {
                    return Err(Error::GenericError(
                        "ORC file is empty or could not be read".to_string(),
                    ));
                }
                ctx.read_batches(batches)
                    .map_err(|e| Error::GenericError(e.to_string()))?
            }
            _ => {
                return Err(Error::GenericError(
                    "Only Parquet, Avro, CSV, and ORC are supported as input file types"
                        .to_string(),
                ));
            }
        };

        if let Some(columns) = &self.select {
            let parsed = parse_select_columns(columns);
            if !parsed.is_empty() {
                let col_refs: Vec<&str> = parsed.iter().map(String::as_str).collect();
                df = df
                    .select_columns(&col_refs)
                    .map_err(|e| Error::GenericError(e.to_string()))?;
            }
        }

        if let Some(n) = self.limit {
            df = df
                .limit(0, Some(n))
                .map_err(|e| Error::GenericError(e.to_string()))?;
        }

        Ok(DataFrameSource::new(df))
    }
}

#[async_trait(?Send)]
impl Step for DataFrameReader {
    type Input = ();
    type Output = DataFrameSource;

    async fn execute(self, _input: Self::Input) -> crate::Result<Self::Output> {
        self.read().await
    }
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion after reading.
fn read_orc_to_batches(path: &str) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
    use crate::pipeline::ReadArgs;

    let args = ReadArgs {
        path: path.to_string(),
        limit: None,
        offset: None,
        csv_has_header: None,
    };
    let reader = orc::read_orc(&args)?;
    let batches: Vec<arrow::record_batch::RecordBatch> =
        reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}
