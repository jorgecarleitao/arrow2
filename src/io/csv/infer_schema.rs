// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use csv::StringRecord;

use crate::datatypes::DataType;
use crate::datatypes::{Field, Schema};
use crate::error::Result;

/// Infer the schema of a CSV file by reading through the first n records of the file,
/// with `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its schema.
///
/// Return infered schema and number of records used for inference.
pub fn infer_file_schema<R: Read + Seek, F: Fn(&str) -> DataType>(
    reader: &mut R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
    infer: &F,
) -> Result<(Schema, usize)> {
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .from_reader(reader);

    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let headers: Vec<String> = if has_header {
        let headers = &csv_reader.headers()?.clone();
        headers.iter().map(|s| s.to_string()).collect()
    } else {
        let first_record_count = &csv_reader.headers()?.len();
        (0..*first_record_count)
            .map(|i| format!("column_{}", i + 1))
            .collect()
    };

    // save the csv reader position after reading headers
    let position = csv_reader.position().clone();

    let header_length = headers.len();
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];
    // keep track of columns with nulls
    let mut nulls: Vec<bool> = vec![false; header_length];

    // return csv reader position to after headers
    csv_reader.seek(position)?;

    let mut records_count = 0;
    let mut fields = vec![];

    let mut record = StringRecord::new();
    let max_records = max_read_records.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !csv_reader.read_record(&mut record)? {
            break;
        }
        records_count += 1;

        for i in 0..header_length {
            if let Some(string) = record.get(i) {
                if string.is_empty() {
                    nulls[i] = true;
                } else {
                    column_types[i].insert(infer(string));
                }
            }
        }
    }

    // build schema from inference results
    for i in 0..header_length {
        let possibilities = &column_types[i];
        let has_nulls = nulls[i];
        let field_name = &headers[i];

        // determine data type based on possible types
        // if there are incompatible types, use DataType::Utf8
        match possibilities.len() {
            1 => {
                for dtype in possibilities.iter() {
                    fields.push(Field::new(&field_name, dtype.clone(), has_nulls));
                }
            }
            2 => {
                if possibilities.contains(&DataType::Int64)
                    && possibilities.contains(&DataType::Float64)
                {
                    // we have an integer and double, fall down to double
                    fields.push(Field::new(&field_name, DataType::Float64, has_nulls));
                } else {
                    // default to Utf8 for conflicting datatypes (e.g bool and int)
                    fields.push(Field::new(&field_name, DataType::Utf8, has_nulls));
                }
            }
            _ => fields.push(Field::new(&field_name, DataType::Utf8, has_nulls)),
        }
    }

    // return the reader seek back to the start
    csv_reader.into_inner().seek(SeekFrom::Start(0))?;

    Ok((Schema::new(fields), records_count))
}

/// Infer schema from a list of CSV files by reading through first n records
/// with `max_read_records` controlling the maximum number of records to read.
///
/// Files will be read in the given order untill n records have been reached.
///
/// If `max_read_records` is not set, all files will be read fully to infer the schema.
pub fn infer_schema_from_files<F: Fn(&str) -> DataType>(
    files: &[String],
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
    infer: F,
) -> Result<Schema> {
    let mut schemas = vec![];
    let mut records_to_read = max_read_records.unwrap_or(std::usize::MAX);

    for fname in files.iter() {
        let (schema, records_read) = infer_file_schema(
            &mut File::open(fname)?,
            delimiter,
            Some(records_to_read),
            has_header,
            &infer,
        )?;
        if records_read == 0 {
            continue;
        }
        schemas.push(schema.clone());
        records_to_read -= records_read;
        if records_to_read == 0 {
            break;
        }
    }

    Schema::try_merge(schemas)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use tempfile::NamedTempFile;

    use crate::io::csv::reader::infer;

    #[test]
    fn test_infer_schema_from_multiple_files() -> Result<()> {
        let mut csv1 = NamedTempFile::new()?;
        let mut csv2 = NamedTempFile::new()?;
        let csv3 = NamedTempFile::new()?; // empty csv file should be skipped
        let mut csv4 = NamedTempFile::new()?;
        writeln!(csv1, "c1,c2,c3")?;
        writeln!(csv1, "1,\"foo\",0.5")?;
        writeln!(csv1, "3,\"bar\",1")?;
        // reading csv2 will set c2 to optional
        writeln!(csv2, "c1,c2,c3,c4")?;
        writeln!(csv2, "10,,3.14,true")?;
        // reading csv4 will set c3 to optional
        writeln!(csv4, "c1,c2,c3")?;
        writeln!(csv4, "10,\"foo\",")?;

        let schema = infer_schema_from_files(
            &[
                csv3.path().to_str().unwrap().to_string(),
                csv1.path().to_str().unwrap().to_string(),
                csv2.path().to_str().unwrap().to_string(),
                csv4.path().to_str().unwrap().to_string(),
            ],
            b',',
            Some(3), // only csv1 and csv2 should be read
            true,
            infer,
        )?;

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(false, schema.field(0).is_nullable());
        assert_eq!(true, schema.field(1).is_nullable());
        assert_eq!(false, schema.field(2).is_nullable());
        assert_eq!(false, schema.field(3).is_nullable());

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Utf8, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());

        Ok(())
    }
}
