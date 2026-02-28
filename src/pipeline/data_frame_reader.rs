use datafusion::execution::context::SessionContext;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::ParquetReadOptions;

use crate::Error;
use crate::FileType;
use crate::cli::convert::DataFrameSource;
use crate::pipeline::Step;
use crate::pipeline::orc;
use crate::utils::parse_select_columns;

/// A step that reads an input file into a DataFusion DataFrame with optional column selection and limit.
pub struct DataFrameReader {
    input_path: String,
    input_file_type: FileType,
    select: Option<Vec<String>>,
    limit: Option<usize>,
}

impl DataFrameReader {
    pub fn new(
        input_path: &str,
        input_file_type: FileType,
        select: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            input_path: input_path.to_string(),
            input_file_type,
            select,
            limit,
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
                    "Only Parquet, Avro, and ORC are supported as input file types".to_string(),
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

impl Step for DataFrameReader {
    type Input = ();
    type Output = DataFrameSource;

    fn execute(self, _input: Self::Input) -> crate::Result<Self::Output> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| handle.block_on(self.read()))
    }
}

/// Creates a `DataFrameReader` that reads an input file into a DataFusion DataFrame.
pub fn read_dataframe(
    input_path: &str,
    input_file_type: FileType,
    select: Option<Vec<String>>,
    limit: Option<usize>,
) -> DataFrameReader {
    DataFrameReader::new(input_path, input_file_type, select, limit)
}

/// Reads an ORC file into record batches (ORC is not natively supported by DataFusion).
/// Limit is applied via DataFusion after reading.
fn read_orc_to_batches(path: &str) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
    use crate::pipeline::ReadArgs;

    let args = ReadArgs {
        path: path.to_string(),
        limit: None,
        offset: None,
    };
    let reader = orc::read_orc(&args)?;
    let batches: Vec<arrow::record_batch::RecordBatch> =
        reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}
