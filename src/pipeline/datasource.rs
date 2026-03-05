use arrow::array::RecordBatchReader;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;

use crate::Error;
use crate::Result;
use crate::pipeline::Source;

/// A source for the next step in the pipeline, either a DataFusion DataFrame or a RecordBatchReader.
pub enum DataSource {
    DataFrame(Option<Box<DataFrame>>),
    RecordBatch(Option<Box<dyn RecordBatchReader>>),
}

impl From<datafusion::dataframe::DataFrame> for DataSource {
    fn from(data: datafusion::dataframe::DataFrame) -> Self {
        Self::DataFrame(Some(Box::new(data)))
    }
}
impl From<Box<dyn RecordBatchReader>> for DataSource {
    fn from(data: Box<dyn RecordBatchReader>) -> Self {
        Self::RecordBatch(Some(data))
    }
}

impl Source<DataFrame> for DataSource {
    fn get(&mut self) -> Result<Box<DataFrame>> {
        match self {
            Self::DataFrame(dataframe) => dataframe
                .take()
                .ok_or_else(|| Error::GenericError("DataFrame already taken".to_string())),
            Self::RecordBatch(reader) => {
                let reader = reader
                    .take()
                    .ok_or_else(|| Error::GenericError("Reader already taken".to_string()))?;
                let mut adapter = RecordBatchToDataFrameAdapter::new(reader);
                let first = adapter
                    .next()
                    .transpose()?
                    .ok_or_else(|| Error::GenericError("Reader returned no batches".to_string()))?;

                let dataframe = adapter.try_fold(first, |acc, next| {
                    let next = next?;
                    acc.union(next)
                        .map_err(|e| Error::GenericError(e.to_string()))
                })?;
                Ok(Box::new(dataframe))
            }
        }
    }
}

/// Adapter that streams `RecordBatchReader` batches as DataFusion `DataFrame`s.
pub struct RecordBatchToDataFrameAdapter {
    ctx: SessionContext,
    reader: Box<dyn RecordBatchReader>,
}

impl RecordBatchToDataFrameAdapter {
    pub fn new(reader: Box<dyn RecordBatchReader>) -> Self {
        Self {
            ctx: SessionContext::new(),
            reader,
        }
    }
}

impl Iterator for RecordBatchToDataFrameAdapter {
    type Item = Result<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.reader.next()?;
        match batch {
            Ok(batch) => Some(
                self.ctx
                    .read_batches(vec![batch])
                    .map_err(|e| Error::GenericError(e.to_string())),
            ),
            Err(e) => Some(Err(Error::ArrowError(e))),
        }
    }
}
