use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int8Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::UInt8Array;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use rust_xlsxwriter::Workbook;
use rust_xlsxwriter::Worksheet;

use crate::Error;
use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteArgs;

/// Pipeline step that writes record batches to an Excel (.xlsx) file.
pub struct WriteXlsxStep {
    pub prev: Box<dyn RecordBatchReaderSource>,
    pub args: WriteArgs,
}

pub struct WriteXlsxResult {}

impl Step for WriteXlsxStep {
    type Input = Box<dyn RecordBatchReaderSource>;
    type Output = WriteXlsxResult;

    fn execute(mut self) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();

        let reader = self.prev.get_record_batch_reader()?;
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
        Ok(WriteXlsxResult {})
    }
}

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
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            worksheet.write(row, col, arr.value(index))?;
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            worksheet.write(row, col, arr.value(index))?;
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as i64)?;
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let v = arr.value(index);
            if v <= i64::MAX as u64 {
                worksheet.write(row, col, v as i64)?;
            } else {
                worksheet.write_string(row, col, v.to_string())?;
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            worksheet.write(row, col, arr.value(index) as f64)?;
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            worksheet.write(row, col, arr.value(index))?;
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            worksheet.write_string(row, col, arr.value(index))?;
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap();
            worksheet.write_string(row, col, arr.value(index))?;
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

fn arrow_temporal_to_chrono(array: &ArrayRef, index: usize) -> Option<chrono::NaiveDateTime> {
    use arrow::array::TimestampMillisecondArray;
    use arrow::array::TimestampSecondArray;

    match array.data_type() {
        DataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                let ts = arr.value(index);
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                let ts = arr.value(index);
                return chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                let ts = arr.value(index) / 1000;
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
            if let Some(arr) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            {
                let ts = arr.value(index) / 1_000_000;
                return chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.naive_utc());
            }
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Date64Array>()?;
            let ms = arr.value(index);
            return chrono::DateTime::from_timestamp_millis(ms).map(|dt| dt.naive_utc());
        }
        _ => {}
    }
    None
}

fn format_arrow_value_unknown(array: &ArrayRef, index: usize) -> String {
    arrow::util::display::array_value_to_string(array.as_ref(), index)
        .unwrap_or_else(|_| "-".to_string())
}
