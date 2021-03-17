//! CSV Writer
//!
//! This CSV writer allows Arrow data (in record batches) to be written as CSV files.
//! The writer does not support writing `ListArray` and `StructArray`.
//!
//! Example:
//!
//! ```
//! use std::fs::File;
//! use std::sync::Arc;
//!
//! use arrow2::array::*;
//! use arrow2::io::csv;
//! use arrow2::datatypes::*;
//! use arrow2::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![
//!     Field::new("c1", DataType::Utf8, false),
//!     Field::new("c2", DataType::Float64, true),
//!     Field::new("c3", DataType::UInt32, false),
//!     Field::new("c3", DataType::Boolean, true),
//! ]);
//! let c1 = Utf8Array::<i32>::from_slice(&[
//!     "Lorem ipsum dolor sit amet",
//!     "consectetur adipiscing elit",
//!     "sed do eiusmod tempor",
//! ]);
//! let c2 = Primitive::from(&[
//!     Some(123.564532f64),
//!     None,
//!     Some(-556132.25),
//! ]).to(DataType::Float64);
//! let c3 = Primitive::from_slice(&[3u32, 2, 1]).to(DataType::UInt32);
//! let c4 = BooleanArray::from(&[Some(true), Some(false), None]);
//!
//! let batch = RecordBatch::try_new(
//!     schema,
//!     vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
//! )
//! .unwrap();
//!
//! use std::io::Cursor;
//! let mut buffer = Cursor::new(Vec::new());
//!
//! let mut writer = csv::Writer::new(buffer);
//! let batches = vec![&batch, &batch];
//! for batch in batches {
//!     writer.write(batch).unwrap();
//! }
//! ```

use csv as csv_crate;

use std::io::Write;

use crate::array::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;
use crate::{datatypes::*, temporal_conversions, types::NativeType, util::lexical_to_string};
const DEFAULT_DATE_FORMAT: &str = "%F";
const DEFAULT_TIME_FORMAT: &str = "%T";
const DEFAULT_TIMESTAMP_FORMAT: &str = "%FT%H:%M:%S.%9f";

fn write_primitive_value<T>(array: &dyn Array, i: usize) -> String
where
    T: NativeType + lexical_core::ToLexical,
{
    let c = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    lexical_to_string(c.value(i))
}

/// A CSV writer
#[derive(Debug)]
pub struct Writer<W: Write> {
    /// The object to write to
    writer: csv_crate::Writer<W>,
    /// Column delimiter. Defaults to `b','`
    delimiter: u8,
    /// Whether file should be written with headers. Defaults to `true`
    has_headers: bool,
    /// The date format for date arrays
    date_format: String,
    /// The timestamp format for timestamp arrays
    timestamp_format: String,
    /// The time format for time arrays
    time_format: String,
    /// Is the beginning-of-writer
    beginning: bool,
}

impl<W: Write> Writer<W> {
    /// Create a new CsvWriter from a writable object, with default options
    pub fn new(writer: W) -> Self {
        let delimiter = b',';
        let mut builder = csv_crate::WriterBuilder::new();
        let writer = builder.delimiter(delimiter).from_writer(writer);
        Writer {
            writer,
            delimiter,
            has_headers: true,
            date_format: DEFAULT_DATE_FORMAT.to_string(),
            time_format: DEFAULT_TIME_FORMAT.to_string(),
            timestamp_format: DEFAULT_TIMESTAMP_FORMAT.to_string(),
            beginning: true,
        }
    }

    /// Convert a record to a string vector
    fn convert(&self, batch: &RecordBatch, row_index: usize, buffer: &mut [String]) -> Result<()> {
        // TODO: it'd be more efficient if we could create `record: Vec<&[u8]>
        for (col_index, item) in buffer.iter_mut().enumerate() {
            let col = batch.column(col_index).as_ref();
            if col.is_null(row_index) {
                // write an empty value
                *item = "".to_string();
                continue;
            }
            *item = match col.data_type() {
                DataType::Float64 => write_primitive_value::<f64>(col, row_index),
                DataType::Float32 => write_primitive_value::<f32>(col, row_index),
                DataType::Int8 => write_primitive_value::<i8>(col, row_index),
                DataType::Int16 => write_primitive_value::<i16>(col, row_index),
                DataType::Int32 => write_primitive_value::<i32>(col, row_index),
                DataType::Int64 => write_primitive_value::<i64>(col, row_index),
                DataType::UInt8 => write_primitive_value::<u8>(col, row_index),
                DataType::UInt16 => write_primitive_value::<u16>(col, row_index),
                DataType::UInt32 => write_primitive_value::<u32>(col, row_index),
                DataType::UInt64 => write_primitive_value::<u64>(col, row_index),
                DataType::Boolean => {
                    let c = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    c.value(row_index).to_string()
                }
                DataType::Utf8 => {
                    let c = col.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                    c.value(row_index).to_owned()
                }
                DataType::LargeUtf8 => {
                    let c = col.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                    c.value(row_index).to_owned()
                }
                DataType::Date32 => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                    temporal_conversions::date32_to_datetime(c.value(row_index))
                        .format(&self.date_format)
                        .to_string()
                }
                DataType::Date64 => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    temporal_conversions::date64_to_datetime(c.value(row_index))
                        .format(&self.date_format)
                        .to_string()
                }
                DataType::Time32(TimeUnit::Second) => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                    temporal_conversions::time32s_to_time(c.value(row_index))
                        .format(&self.time_format)
                        .to_string()
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
                    temporal_conversions::time32ms_to_time(c.value(row_index))
                        .format(&self.time_format)
                        .to_string()
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    temporal_conversions::time64us_to_time(c.value(row_index))
                        .format(&self.time_format)
                        .to_string()
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
                    temporal_conversions::time64ns_to_time(c.value(row_index))
                        .format(&self.time_format)
                        .to_string()
                }
                DataType::Timestamp(time_unit, _) => {
                    let c = col.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();

                    let datetime = match time_unit {
                        TimeUnit::Second => {
                            temporal_conversions::timestamp_s_to_datetime(c.value(row_index))
                        }
                        TimeUnit::Millisecond => {
                            temporal_conversions::timestamp_ms_to_datetime(c.value(row_index))
                        }
                        TimeUnit::Microsecond => {
                            temporal_conversions::timestamp_us_to_datetime(c.value(row_index))
                        }
                        TimeUnit::Nanosecond => {
                            temporal_conversions::timestamp_ns_to_datetime(c.value(row_index))
                        }
                    };
                    format!("{}", datetime.format(&self.timestamp_format))
                }
                t => {
                    // List and Struct arrays not supported by the writer, any
                    // other type needs to be implemented
                    return Err(ArrowError::NotYetImplemented(format!(
                        "CSV Writer does not support {:?} data type",
                        t
                    )));
                }
            };
        }
        Ok(())
    }

    /// Write a vector of record batches to a writable object
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let num_columns = batch.num_columns();
        if self.beginning {
            if self.has_headers {
                let mut headers: Vec<String> = Vec::with_capacity(num_columns);
                batch
                    .schema()
                    .fields()
                    .iter()
                    .for_each(|field| headers.push(field.name().to_string()));
                self.writer.write_record(&headers[..])?;
            }
            self.beginning = false;
        }

        let mut buffer = vec!["".to_string(); batch.num_columns()];

        for row_index in 0..batch.num_rows() {
            self.convert(batch, row_index, &mut buffer)?;
            self.writer.write_record(&buffer)?;
        }
        self.writer.flush()?;

        Ok(())
    }
}

/// A CSV writer builder
#[derive(Debug)]
pub struct WriterBuilder {
    /// Optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Whether to write column names as file headers. Defaults to `true`
    has_headers: bool,
    /// Optional date format for date arrays
    date_format: Option<String>,
    /// Optional timestamp format for timestamp arrays
    timestamp_format: Option<String>,
    /// Optional time format for time arrays
    time_format: Option<String>,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self {
            has_headers: true,
            delimiter: None,
            date_format: Some(DEFAULT_DATE_FORMAT.to_string()),
            time_format: Some(DEFAULT_TIME_FORMAT.to_string()),
            timestamp_format: Some(DEFAULT_TIMESTAMP_FORMAT.to_string()),
        }
    }
}

impl WriterBuilder {
    /// Create a new builder for configuring CSV writing options.
    ///
    /// To convert a builder into a writer, call `WriterBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// use arrow2::io::csv;
    /// use std::fs::File;
    ///
    /// let file = File::create("target/out.csv").unwrap();
    /// let builder = csv::WriterBuilder::new().has_headers(false);    
    /// let writer = builder.build(file);
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to write headers
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the CSV file's date format
    pub fn with_date_format(mut self, format: String) -> Self {
        self.date_format = Some(format);
        self
    }

    /// Set the CSV file's time format
    pub fn with_time_format(mut self, format: String) -> Self {
        self.time_format = Some(format);
        self
    }

    /// Set the CSV file's timestamp format
    pub fn with_timestamp_format(mut self, format: String) -> Self {
        self.timestamp_format = Some(format);
        self
    }

    /// Create a new `Writer`
    pub fn build<W: Write>(self, writer: W) -> Writer<W> {
        let delimiter = self.delimiter.unwrap_or(b',');
        let mut builder = csv_crate::WriterBuilder::new();
        let writer = builder.delimiter(delimiter).from_writer(writer);
        Writer {
            writer,
            delimiter,
            has_headers: self.has_headers,
            date_format: self
                .date_format
                .unwrap_or_else(|| DEFAULT_DATE_FORMAT.to_string()),
            time_format: self
                .time_format
                .unwrap_or_else(|| DEFAULT_TIME_FORMAT.to_string()),
            timestamp_format: self
                .timestamp_format
                .unwrap_or_else(|| DEFAULT_TIMESTAMP_FORMAT.to_string()),
            beginning: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datatypes::{Field, Schema};
    use crate::util::test_util::get_temp_file;
    use std::io::Read;
    use std::sync::Arc;
    use std::{fs::File, io::Cursor};

    #[test]
    fn test_write_csv() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = Utf8Array::<i32>::from_slice([
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = Primitive::<f64>::from([Some(123.564532), None, Some(-556132.25)])
            .to(DataType::Float64);
        let c3 = Primitive::<u32>::from_slice(vec![3, 2, 1]).to(DataType::UInt32);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c5 = Primitive::<i64>::from([None, Some(1555584887378), Some(1555555555555)])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None));
        let c6 = Primitive::<i32>::from_slice(vec![1234, 24680, 85563])
            .to(DataType::Time32(TimeUnit::Second));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c5),
                Arc::new(c6),
            ],
        )
        .unwrap();

        let file = get_temp_file("columns.csv", &[]);

        let mut writer = Writer::new(file);
        let batches = vec![&batch, &batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }
        // check that file was written successfully
        let mut file = File::open("target/debug/testdata/columns.csv").unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            r#"c1,c2,c3,c4,c5,c6
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03
"#
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_write_csv_custom_options() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = Utf8Array::<i32>::from_slice([
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = Primitive::<f64>::from([Some(123.564532), None, Some(-556132.25)])
            .to(DataType::Float64);
        let c3 = Primitive::<u32>::from_slice([3, 2, 1]).to(DataType::UInt32);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c6 = Primitive::<i32>::from_slice([1234, 24680, 85563])
            .to(DataType::Time32(TimeUnit::Second));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c6),
            ],
        )
        .unwrap();

        let file = get_temp_file("custom_options.csv", &[]);

        let builder = WriterBuilder::new()
            .has_headers(false)
            .with_delimiter(b'|')
            .with_time_format("%r".to_string());
        let mut writer = builder.build(file);
        let batches = vec![&batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }

        // check that file was written successfully
        let mut file = File::open("target/debug/testdata/custom_options.csv").unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "Lorem ipsum dolor sit amet|123.564532|3|true|12:20:34 AM\nconsectetur adipiscing elit||2|false|06:51:20 AM\nsed do eiusmod tempor|-556132.25|1||11:46:03 PM\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_export_csv_string() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);

        let c1 = Utf8Array::<i32>::from_slice([
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = Primitive::<f64>::from(vec![Some(123.564532), None, Some(-556132.25)])
            .to(DataType::Float64);
        let c3 = Primitive::<u32>::from_slice(&[3, 2, 1]).to(DataType::UInt32);
        let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
        let c5 = Primitive::<i64>::from(vec![None, Some(1555584887378), Some(1555555555555)])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None));
        let c6 = Primitive::<i32>::from_slice([1234, 24680, 85563])
            .to(DataType::Time32(TimeUnit::Second));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
                Arc::new(c5),
                Arc::new(c6),
            ],
        )
        .unwrap();

        let mut writer = Writer::new(Cursor::new(vec![]));
        let batches = vec![&batch, &batch];
        for batch in batches {
            writer.write(batch).unwrap();
        }

        let left = "c1,c2,c3,c4,c5,c6
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03\n";
        let right = writer
            .writer
            .into_inner()
            .map(|s| String::from_utf8(s.into_inner()).unwrap());
        assert_eq!(Some(left.to_string()), right.ok());
    }
}
