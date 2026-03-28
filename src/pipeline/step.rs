//! [`Step`] and [`Producer`] traits for pipeline composition.

use arrow::array::RecordBatchReader;
use async_trait::async_trait;

use crate::Result;

/// A `Step` defines a step in the pipeline that can be executed
/// and has an input and output type.
#[async_trait(?Send)]
pub trait Step {
    type Input;
    type Output;

    /// Execute the step
    async fn execute(self, input: Self::Input) -> Result<Self::Output>;
}

/// A producer produces a value of type `T`.
#[async_trait(?Send)]
pub trait Producer<T: ?Sized> {
    /// Produces the next value from this source; consumes the source on first call.
    async fn get(&mut self) -> Result<Box<T>>;
}

/// Type alias for a boxed source of `RecordBatchReader`.
pub type RecordBatchReaderSource = Box<dyn Producer<dyn RecordBatchReader + 'static>>;
