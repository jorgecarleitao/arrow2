use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use super::{ByteRecord, Reader};

use crate::datatypes::{DataType, TimeUnit};
use crate::datatypes::{Field, Schema};
use crate::error::Result;

pub(super) const RFC3339: &str = "%Y-%m-%dT%H:%M:%S%.f%:z";

fn is_boolean(bytes: &[u8]) -> bool {
    bytes.eq_ignore_ascii_case(b"true") | bytes.eq_ignore_ascii_case(b"false")
}

fn is_float(bytes: &[u8]) -> bool {
    lexical_core::parse::<f64>(bytes).is_ok()
}

fn is_integer(bytes: &[u8]) -> bool {
    lexical_core::parse::<i64>(bytes).is_ok()
}

fn is_date(string: &str) -> bool {
    string.parse::<chrono::NaiveDate>().is_ok()
}

fn is_time(string: &str) -> bool {
    string.parse::<chrono::NaiveTime>().is_ok()
}

fn is_naive_datetime(string: &str) -> bool {
    string.parse::<chrono::NaiveDateTime>().is_ok()
}

fn is_datetime(string: &str) -> Option<String> {
    let mut parsed = chrono::format::Parsed::new();
    let fmt = chrono::format::StrftimeItems::new(RFC3339);
    if chrono::format::parse(&mut parsed, string, fmt).is_ok() {
        parsed.offset.map(|x| {
            let hours = x / 60 / 60;
            let minutes = x / 60 - hours * 60;
            format!("{:03}:{:02}", hours, minutes)
        })
    } else {
        None
    }
}

/// Infers [`DataType`] from `bytes`
/// # Implementation
/// * case insensitive "true" or "false" are mapped to [`DataType::Boolean`]
/// * parsable to integer is mapped to [`DataType::Int64`]
/// * parsable to float is mapped to [`DataType::Float64`]
/// * parsable to date is mapped to [`DataType::Date32`]
/// * parsable to time is mapped to [`DataType::Time32(TimeUnit::Millisecond)`]
/// * parsable to naive datetime is mapped to [`DataType::Timestamp(TimeUnit::Millisecond, None)`]
/// * parsable to time-aware datetime is mapped to [`DataType::Timestamp`] of milliseconds and parsed offset.
/// * other utf8 is mapped to [`DataType::Utf8`]
/// * invalid utf8 is mapped to [`DataType::Binary`]
pub fn infer(bytes: &[u8]) -> DataType {
    if is_boolean(bytes) {
        DataType::Boolean
    } else if is_integer(bytes) {
        DataType::Int64
    } else if is_float(bytes) {
        DataType::Float64
    } else if let Ok(string) = simdutf8::basic::from_utf8(bytes) {
        if is_date(string) {
            DataType::Date32
        } else if is_time(string) {
            DataType::Time32(TimeUnit::Millisecond)
        } else if is_naive_datetime(string) {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        } else if let Some(offset) = is_datetime(string) {
            DataType::Timestamp(TimeUnit::Millisecond, Some(offset))
        } else {
            DataType::Utf8
        }
    } else {
        // invalid utf8
        DataType::Binary
    }
}

/// Infers a [`Schema`] of a CSV file by reading through the first n records up to `max_rows`.
/// Seeks back to the begining of the file _after_ the header
pub fn infer_schema<R: Read + Seek, F: Fn(&[u8]) -> DataType>(
    reader: &mut Reader<R>,
    max_rows: Option<usize>,
    has_header: bool,
    infer: &F,
) -> Result<Schema> {
    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let headers: Vec<String> = if has_header {
        reader.headers()?.iter().map(|s| s.to_string()).collect()
    } else {
        let first_record_count = &reader.headers()?.len();
        (0..*first_record_count)
            .map(|i| format!("column_{}", i + 1))
            .collect()
    };

    // save the csv reader position after reading headers
    let position = reader.position().clone();

    let header_length = headers.len();
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];

    let mut records_count = 0;

    let mut record = ByteRecord::new();
    let max_records = max_rows.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !reader.read_byte_record(&mut record)? {
            break;
        }
        records_count += 1;

        for (i, column) in column_types.iter_mut().enumerate() {
            if let Some(string) = record.get(i) {
                column.insert(infer(string));
            }
        }
    }

    // build schema from inference results
    let fields = headers
        .iter()
        .zip(column_types.into_iter())
        .map(|(field_name, mut possibilities)| {
            // determine data type based on possible types
            // if there are incompatible types, use DataType::Utf8
            let data_type = match possibilities.len() {
                1 => possibilities.drain().next().unwrap(),
                2 => {
                    if possibilities.contains(&DataType::Int64)
                        && possibilities.contains(&DataType::Float64)
                    {
                        // we have an integer and double, fall down to double
                        DataType::Float64
                    } else {
                        // default to Utf8 for conflicting datatypes (e.g bool and int)
                        DataType::Utf8
                    }
                }
                _ => DataType::Utf8,
            };
            Field::new(field_name, data_type, true)
        })
        .collect();

    // return the reader seek back to the start
    reader.seek(position)?;

    Ok(Schema::new(fields))
}
