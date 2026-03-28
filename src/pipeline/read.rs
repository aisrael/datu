use std::fs::File;

use async_trait::async_trait;
use datafusion::prelude::AvroReadOptions;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::DataFrame;
use datafusion::prelude::NdJsonReadOptions;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use orc_rust::ArrowReaderBuilder;

use crate::Error;
use crate::FileType;
use crate::Result;
use crate::errors::PipelineExecutionError;
use crate::errors::PipelinePlanningError;
use crate::pipeline::Producer;
use crate::pipeline::Step;
use crate::pipeline::dataframe::DataFrameSource;

/// Arguments for reading a file (all formats).
///
/// ## Record-batch `offset` / `limit`
///
/// For [`crate::pipeline::build_reader`], [`read_parquet`](crate::pipeline::parquet::read_parquet),
/// [`read_avro`](crate::pipeline::avro::read_avro), [`read_orc`](crate::pipeline::orc::read_orc), and
/// the CSV record-batch step, **`offset`** skips that many rows from the start of the file (default
/// `0` when unset) and **`limit`** caps how many rows are returned after that (`None` = no cap). This
/// matches SQL-style `OFFSET` / `LIMIT`. Parquet and ORC may apply these via native row selection or
/// pushdown; other formats use the same semantics with streaming adapters in
/// [`crate::pipeline::record_batch`].
///
/// In the record-batch **pipeline** [`crate::pipeline::RecordBatchHead`] applies a row cap from the
/// current reader position; [`crate::pipeline::RecordBatchTail`] keeps the **last** `n` rows only
/// and is not the same as `offset`/`limit` from the start of the file.
///
/// [`read`](read) and [`read_to_dataframe`] do **not** apply [`ReadArgs::limit`] or [`ReadArgs::offset`].
#[derive(Clone)]
pub struct ReadArgs {
    pub path: String,
    pub file_type: FileType,
    /// When reading CSV: has_header for CsvReadOptions. None is treated as true.
    pub csv_has_header: Option<bool>,
    /// Maximum rows for record-batch reads. None means read all.
    pub limit: Option<usize>,
    /// Rows to skip for record-batch reads (see struct-level docs).
    pub offset: Option<usize>,
}

impl ReadArgs {
    /// Builds read arguments with no CSV override and no row slice.
    pub fn new(path: impl Into<String>, file_type: FileType) -> Self {
        Self {
            path: path.into(),
            file_type,
            csv_has_header: None,
            limit: None,
            offset: None,
        }
    }
}

pub(crate) fn expect_file_type(args: &ReadArgs, expected: FileType) -> Result<()> {
    if args.file_type != expected {
        return Err(Error::GenericError(format!(
            "read args file type mismatch: expected {expected}, got {}",
            args.file_type
        )));
    }
    Ok(())
}

/// Outcome of [`read`](read): a DataFusion source or an ORC reader builder.
#[allow(clippy::large_enum_variant)]
pub enum ReadResult {
    DataFrame(DataFrameSource),
    OrcReaderBuilder(ArrowReaderBuilder<File>),
}

impl std::fmt::Debug for ReadResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadResult::DataFrame(source) => write!(f, "DataFrame({:?})", &source),
            ReadResult::OrcReaderBuilder(builder) => write!(f, "OrcReaderBuilder({:p})", &builder),
        }
    }
}

/// Read a file and return a [ReadResult].
///
/// Does not apply [`ReadArgs::limit`] or [`ReadArgs::offset`]; use record-batch APIs for slicing.
pub async fn read(args: &ReadArgs) -> Result<ReadResult> {
    if args.file_type.supports_datafusion_file_read() {
        read_to_dataframe(&args.path, args.file_type, args.csv_has_header).await
    } else if args.file_type == FileType::Orc {
        read_to_record_batches(args)
    } else {
        Err(Error::PipelinePlanningError(
            PipelinePlanningError::UnsupportedInputFileType(args.file_type.to_string()),
        ))
    }
}

/// Loads supported formats into a [`DataFrameSource`] via DataFusion (not ORC).
///
/// Supported: [`FileType::Parquet`], [`FileType::Avro`], [`FileType::Csv`], [`FileType::Json`].
/// For CSV, `csv_has_header` defaults to `true` when `None`.
///
/// Does not apply [`ReadArgs::limit`] or [`ReadArgs::offset`].
pub async fn read_to_dataframe(
    input_path: &str,
    file_type: FileType,
    csv_has_header: Option<bool>,
) -> Result<ReadResult> {
    let ctx = SessionContext::new();
    let df = match file_type {
        FileType::Parquet => {
            ctx.read_parquet(input_path, ParquetReadOptions::default())
                .await?
        }
        FileType::Avro => {
            ctx.read_avro(input_path, AvroReadOptions::default())
                .await?
        }
        FileType::Json => {
            ctx.read_json(input_path, NdJsonReadOptions::default())
                .await?
        }
        FileType::Csv => {
            let csv_options = CsvReadOptions::new().has_header(csv_has_header.unwrap_or(true));
            ctx.read_csv(input_path, csv_options).await?
        }
        _ => {
            return Err(Error::PipelineExecutionError(
                PipelineExecutionError::UnsupportedInputFileType(file_type),
            ));
        }
    };
    Ok(ReadResult::DataFrame(DataFrameSource::new(df)))
}

/// Pipeline step that reads a file into a DataFusion [`DataFrame`] via [`read_to_dataframe`].
///
/// [`ReadArgs::file_type`] selects the format (Parquet, Avro, CSV, JSON). [`ReadArgs::offset`] and
/// [`ReadArgs::limit`] are not applied; use record-batch readers for row slicing.
pub struct DataframeFormatReader {
    pub args: ReadArgs,
}

#[async_trait(?Send)]
impl Step for DataframeFormatReader {
    type Input = ();
    type Output = DataFrameSource;

    async fn execute(self, _input: Self::Input) -> Result<Self::Output> {
        let result = read_to_dataframe(
            &self.args.path,
            self.args.file_type,
            self.args.csv_has_header,
        )
        .await?;
        let ReadResult::DataFrame(source) = result else {
            unreachable!()
        };
        Ok(source)
    }
}

#[async_trait(?Send)]
impl Producer<DataFrame> for DataframeFormatReader {
    async fn get(&mut self) -> Result<Box<DataFrame>> {
        let result = read_to_dataframe(
            &self.args.path,
            self.args.file_type,
            self.args.csv_has_header,
        )
        .await?;
        let ReadResult::DataFrame(mut source) = result else {
            unreachable!()
        };
        source.get().await
    }
}

/// Opens ORC for record-batch reading; errors for other file types.
pub fn read_to_record_batches(args: &ReadArgs) -> Result<ReadResult> {
    match args.file_type {
        FileType::Orc => {
            let file = std::fs::File::open(&args.path).map_err(Error::IoError)?;
            let builder = ArrowReaderBuilder::try_new(file).map_err(Error::OrcError)?;
            Ok(ReadResult::OrcReaderBuilder(builder))
        }
        _ => Err(Error::PipelineExecutionError(
            PipelineExecutionError::UnsupportedInputFileType(args.file_type),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_parquet() {
        let args = ReadArgs::new("fixtures/table.parquet", FileType::Parquet);
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        source.get().await.expect("expected DataFrame");
    }

    #[tokio::test]
    async fn test_read_avro() {
        let args = ReadArgs::new("fixtures/userdata5.avro", FileType::Avro);
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().await.is_ok());
    }

    #[tokio::test]
    async fn test_read_csv() {
        let args = ReadArgs::new("fixtures/table.csv", FileType::Csv);
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().await.is_ok());
    }

    #[tokio::test]
    async fn test_read_json() {
        let args = ReadArgs::new("fixtures/table.json", FileType::Json);
        let result = read(&args).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::DataFrame(_)));
        let ReadResult::DataFrame(mut source) = result else {
            panic!("expected DataFrame");
        };
        assert!(source.get().await.is_ok());
    }

    #[tokio::test]
    async fn test_read_orc() {
        let args = ReadArgs::new("fixtures/userdata.orc", FileType::Orc);
        let result = read(&args).await;
        println!("result: {:?}", result);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, ReadResult::OrcReaderBuilder(_)));
    }
}
