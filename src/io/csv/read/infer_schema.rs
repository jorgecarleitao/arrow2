use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use super::Reader;
use csv::StringRecord;

use crate::datatypes::DataType;
use crate::datatypes::{Field, Schema};
use crate::error::Result;

/// Infer the schema of a CSV file by reading through the first n records of the file,
/// with `max_rows` controlling the maximum number of records to read.
///
/// If `max_rows` is not set, the whole file is read to infer its schema.
///
/// Return infered schema and number of records used for inference.
pub fn infer_schema<R: Read + Seek, F: Fn(&str) -> DataType>(
    reader: &mut Reader<R>,
    max_rows: Option<usize>,
    has_header: bool,
    infer: &F,
) -> Result<Schema> {
    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let headers: Vec<String> = if has_header {
        let headers = &reader.headers()?.clone();
        headers.iter().map(|s| s.to_string()).collect()
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
    let mut fields = vec![];

    let mut record = StringRecord::new();
    let max_records = max_rows.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !reader.read_record(&mut record)? {
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
    for i in 0..header_length {
        let possibilities = &column_types[i];
        let field_name = &headers[i];

        // determine data type based on possible types
        // if there are incompatible types, use DataType::Utf8
        match possibilities.len() {
            1 => {
                for dtype in possibilities.iter() {
                    fields.push(Field::new(field_name, dtype.clone(), true));
                }
            }
            2 => {
                if possibilities.contains(&DataType::Int64)
                    && possibilities.contains(&DataType::Float64)
                {
                    // we have an integer and double, fall down to double
                    fields.push(Field::new(field_name, DataType::Float64, true));
                } else {
                    // default to Utf8 for conflicting datatypes (e.g bool and int)
                    fields.push(Field::new(field_name, DataType::Utf8, true));
                }
            }
            _ => fields.push(Field::new(field_name, DataType::Utf8, true)),
        }
    }

    // return the reader seek back to the start
    reader.seek(position)?;

    Ok(Schema::new(fields))
}
