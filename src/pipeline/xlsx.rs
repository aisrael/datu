use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::array::RecordBatchReader;
use arrow::datatypes::DataType;
use arrow::datatypes::Date64Type;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::TimestampMicrosecondType;
use arrow::datatypes::TimestampMillisecondType;
use arrow::datatypes::TimestampNanosecondType;
use arrow::datatypes::TimestampSecondType;
use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use rust_xlsxwriter::Workbook;
use rust_xlsxwriter::Worksheet;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::write::WriteArgs;
use crate::pipeline::write::WriteResult;

/// Pipeline step that writes record batches to an Excel (.xlsx) file.
pub struct WriteXlsxStep {
    pub args: WriteArgs,
    pub source: RecordBatchReaderSource,
}

/// Write record batches from a reader to an Excel (.xlsx) file.
pub fn write_record_batch_to_xlsx(path: &str, reader: &mut dyn RecordBatchReader) -> Result<()> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    let schema = reader.schema();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut excel_row: u32 = 0;
    for (col, name) in column_names.iter().enumerate() {
        worksheet.write_string(excel_row, col as u16, *name)?;
    }
    excel_row += 1;

    for batch in reader {
        let batch = batch.map_err(Error::ArrowError)?;
        let batch_row_count = batch.num_rows();
        for batch_row in 0..batch_row_count {
            for (col, array) in batch.columns().iter().enumerate() {
                write_arrow_cell(worksheet, excel_row, col as u16, array, batch_row)?;
            }
            excel_row += 1;
        }
    }

    workbook.save(path)?;
    Ok(())
}

#[async_trait(?Send)]
impl Step for WriteXlsxStep {
    type Input = ();
    type Output = WriteResult;

    async fn execute(mut self, _input: Self::Input) -> Result<Self::Output> {
        let mut reader = self.source.get().await?;
        write_record_batch_to_xlsx(&self.args.path, &mut *reader)?;
        Ok(WriteResult)
    }
}

/// Writes a single Arrow array cell value to an Excel worksheet cell.
fn write_arrow_cell(
    worksheet: &mut Worksheet,
    row: u32,
    col: u16,
    array: &ArrayRef,
    index: usize,
) -> Result<()> {
    if array.is_null(index) {
        worksheet.write_string(row, col, "")?;
        return Ok(());
    }

    match array.data_type() {
        DataType::Boolean => {
            worksheet.write(row, col, array.as_boolean().value(index))?;
        }
        DataType::Int8 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<Int8Type>().value(index) as i64,
            )?;
        }
        DataType::Int16 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<Int16Type>().value(index) as i64,
            )?;
        }
        DataType::Int32 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<Int32Type>().value(index) as i64,
            )?;
        }
        DataType::Int64 => {
            worksheet.write(row, col, array.as_primitive::<Int64Type>().value(index))?;
        }
        DataType::UInt8 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<UInt8Type>().value(index) as i64,
            )?;
        }
        DataType::UInt16 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<UInt16Type>().value(index) as i64,
            )?;
        }
        DataType::UInt32 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<UInt32Type>().value(index) as i64,
            )?;
        }
        DataType::UInt64 => {
            let v = array.as_primitive::<UInt64Type>().value(index);
            if v <= i64::MAX as u64 {
                worksheet.write(row, col, v as i64)?;
            } else {
                worksheet.write_string(row, col, v.to_string())?;
            }
        }
        DataType::Float32 => {
            worksheet.write(
                row,
                col,
                array.as_primitive::<Float32Type>().value(index) as f64,
            )?;
        }
        DataType::Float64 => {
            worksheet.write(row, col, array.as_primitive::<Float64Type>().value(index))?;
        }
        DataType::Utf8 => {
            worksheet.write_string(row, col, array.as_string::<i32>().value(index))?;
        }
        DataType::LargeUtf8 => {
            worksheet.write_string(row, col, array.as_string::<i64>().value(index))?;
        }
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
            if let Some(dt) = arrow_temporal_to_chrono(array, index) {
                worksheet.write(row, col, &dt)?;
            } else {
                worksheet.write_string(row, col, format_arrow_value_unknown(array, index))?;
            }
        }
        _ => {
            worksheet.write_string(row, col, format_arrow_value_unknown(array, index))?;
        }
    }
    Ok(())
}

/// Converts an Arrow temporal/date array value at the given index to a chrono datetime.
fn arrow_temporal_to_chrono(array: &ArrayRef, index: usize) -> Option<chrono::NaiveDateTime> {
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_primitive_opt::<TimestampMillisecondType>() {
                let ts = arr.value(index);
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array.as_primitive_opt::<TimestampSecondType>() {
                let ts = arr.value(index);
                return chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array.as_primitive_opt::<TimestampMicrosecondType>() {
                let ts = arr.value(index) / 1000;
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array.as_primitive_opt::<TimestampNanosecondType>() {
                let ts = arr.value(index) / 1_000_000;
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
        }
        DataType::Date64 => {
            let arr = array.as_primitive_opt::<Date64Type>()?;
            let ms = arr.value(index);
            return chrono::DateTime::from_timestamp_millis(ms).map(|dt| dt.naive_utc());
        }
        _ => {}
    }
    None
}

/// Formats an Arrow array value as a string for types not directly supported by Excel.
fn format_arrow_value_unknown(array: &ArrayRef, index: usize) -> String {
    arrow::util::display::array_value_to_string(array.as_ref(), index)
        .unwrap_or_else(|_| "-".to_string())
}
