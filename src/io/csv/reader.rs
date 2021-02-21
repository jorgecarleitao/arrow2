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

use std::io::Read;
use std::sync::Arc;
use std::{fmt, io::Seek};

use csv;
use csv::{ByteRecord, StringRecord};
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use crate::record_batch::RecordBatch;
use crate::{
    array::Array,
    datatypes::*,
    error::{ArrowError, Result},
};

use super::{
    infer_file_schema, new_boolean_array, new_primitive_array,
    parser::{DefaultParser, GenericParser},
};

// optional bounds of the reader, of the form (min line, max line).
type Bounds = Option<(usize, usize)>;

/// CSV file reader
pub struct Reader<R: Read, P: GenericParser<ArrowError>> {
    /// Explicit schema for the CSV file
    schema: Schema,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// File reader
    reader: csv::Reader<R>,
    /// Current line number
    line_number: usize,
    /// Maximum number of rows to read
    end: usize,
    /// Number of records per batch
    batch_size: usize,
    /// Vector that can hold the `StringRecord`s of the batches
    batch_records: Vec<StringRecord>,

    /// the parser to use
    parser: P,
}

impl<R, P> fmt::Debug for Reader<R, P>
where
    R: Read,
    P: GenericParser<ArrowError>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("line_number", &self.line_number)
            .finish()
    }
}

impl<R: Read, P: GenericParser<ArrowError>> Reader<R, P> {
    /// Create a new CsvReader from any value that implements the `Read` trait.
    ///
    /// If reading a `File` or an input that supports `std::io::Read` and `std::io::Seek`;
    /// you can customise the Reader, such as to enable schema inference, use
    /// `ReaderBuilder`.
    pub fn new(
        reader: R,
        schema: Schema,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
        parser: P,
    ) -> Self {
        let schema = match &projection {
            Some(projection) => {
                let fields = schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Schema::new(projected_fields)
            }
            None => schema,
        };

        Self::from_reader(
            reader, schema, has_header, delimiter, batch_size, bounds, projection, parser,
        )
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Create a new CsvReader from a Reader
    ///
    /// This constructor allows you more flexibility in what records are processed by the
    /// csv reader.
    pub fn from_reader(
        reader: R,
        schema: Schema,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
        parser: P,
    ) -> Self {
        let mut reader_builder = csv::ReaderBuilder::new();
        reader_builder.has_headers(has_header);

        if let Some(c) = delimiter {
            reader_builder.delimiter(c);
        }

        let mut csv_reader = reader_builder.from_reader(reader);

        let (start, end) = match bounds {
            None => (0, usize::MAX),
            Some((start, end)) => (start, end),
        };

        // First we will skip `start` rows
        // note that this skips by iteration. This is because in general it is not possible
        // to seek in CSV. However, skiping still saves the burden of creating arrow arrays,
        // which is a slow operation that scales with the number of columns

        let mut record = ByteRecord::new();
        // Skip first start items
        for _ in 0..start {
            let res = csv_reader.read_byte_record(&mut record);
            if !res.unwrap_or(false) {
                break;
            }
        }

        // Initialize batch_records with StringRecords so they
        // can be reused accross batches
        let mut batch_records = Vec::with_capacity(batch_size);
        batch_records.resize_with(batch_size, Default::default);

        Self {
            schema,
            projection,
            reader: csv_reader,
            line_number: if has_header { start + 1 } else { start },
            batch_size,
            end,
            batch_records,
            parser,
        }
    }
}

impl<R: Read, P: GenericParser<ArrowError>> Iterator for Reader<R, P> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.end - self.line_number;

        let mut read_records = 0;
        for i in 0..std::cmp::min(self.batch_size, remaining) {
            match self.reader.read_record(&mut self.batch_records[i]) {
                Ok(true) => {
                    read_records += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    return Some(Err(ArrowError::External(
                        format!(" at line {}", self.line_number + i,),
                        Box::new(e),
                    )))
                }
            }
        }

        // return early if no data was loaded
        if read_records == 0 {
            return None;
        }

        // parse the batches into a RecordBatch
        let result = parse(
            &self.batch_records[..read_records],
            &self.schema.fields(),
            &self.projection,
            self.line_number,
            &self.parser,
        );

        self.line_number += read_records;

        Some(result)
    }
}

macro_rules! primitive {
    ($type:ty, $line_number:expr, $rows:expr, $i:expr, $data_type:expr, $parser:expr) => {
        new_primitive_array::<$type, ArrowError, _>($line_number, $rows, $i, $data_type, $parser)
            .map(|x| Arc::new(x) as Arc<dyn Array>)
    };
}

/// parses a slice of [csv_crate::StringRecord] into a [array::record_batch::RecordBatch].
fn parse<P: GenericParser<ArrowError>>(
    rows: &[StringRecord],
    fields: &[Field],
    projection: &Option<Vec<usize>>,
    line_number: usize,
    parser: &P,
) -> Result<RecordBatch> {
    let projection: Vec<usize> = match projection {
        Some(ref v) => v.clone(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };

    let arrays: Result<Vec<Arc<dyn Array>>> = projection
        .iter()
        .map(|i| {
            let i = *i;
            let field = &fields[i];
            let data_type = field.data_type();
            match data_type {
                DataType::Boolean => new_boolean_array(line_number, rows, i, parser)
                    .map(|x| Arc::new(x) as Arc<dyn Array>),
                DataType::Int8 => {
                    primitive!(i8, line_number, rows, i, data_type, parser)
                }
                DataType::Int16 => primitive!(i16, line_number, rows, i, data_type, parser),
                DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                    primitive!(i32, line_number, rows, i, data_type, parser)
                }
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Timestamp(_, None) => {
                    primitive!(i64, line_number, rows, i, data_type, parser)
                }
                DataType::UInt8 => primitive!(u8, line_number, rows, i, data_type, parser),
                DataType::UInt16 => primitive!(u16, line_number, rows, i, data_type, parser),
                DataType::UInt32 => primitive!(u32, line_number, rows, i, data_type, parser),
                DataType::UInt64 => primitive!(u64, line_number, rows, i, data_type, parser),
                DataType::Float32 => primitive!(f32, line_number, rows, i, data_type, parser),
                DataType::Float64 => primitive!(f64, line_number, rows, i, data_type, parser),
                other => Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported data type {:?}",
                    other
                ))),
            }
        })
        .collect();

    let projected_fields: Vec<Field> = projection.iter().map(|i| fields[*i].clone()).collect();

    let projected_schema = Schema::new(projected_fields);

    arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr))
}

lazy_static! {
    static ref DECIMAL_RE: Regex = Regex::new(r"^-?(\d+\.\d+)$").unwrap();
    static ref INTEGER_RE: Regex = Regex::new(r"^-?(\d+)$").unwrap();
    static ref BOOLEAN_RE: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\d$").unwrap();
    static ref DATETIME_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$").unwrap();
}

/// Infer the data type of a record
pub fn infer(string: &str) -> DataType {
    // when quoting is enabled in the reader, these quotes aren't escaped, we default to
    // Utf8 for them
    if string.starts_with('"') {
        return DataType::Utf8;
    }
    // match regex in a particular order
    if BOOLEAN_RE.is_match(string) {
        DataType::Boolean
    } else if DECIMAL_RE.is_match(string) {
        DataType::Float64
    } else if INTEGER_RE.is_match(string) {
        DataType::Int64
    } else if DATETIME_RE.is_match(string) {
        DataType::Date64
    } else if DATE_RE.is_match(string) {
        DataType::Date32
    } else {
        DataType::Utf8
    }
}

/// CSV file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the CSV file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the CSV structure.
    schema: Option<Schema>,
    /// Whether the file has headers or not
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// The bounds over which to scan the reader. `None` starts from 0 and runs until EOF.
    bounds: Bounds,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            has_header: false,
            delimiter: None,
            max_records: None,
            batch_size: 1024,
            bounds: None,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring CSV parsing options.
    ///
    /// To convert a builder into a reader, call `ReaderBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate arrow;
    ///
    /// use arrow::csv;
    /// use std::fs::File;
    ///
    /// fn example() -> csv::Reader<File> {
    ///     let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = csv::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> ReaderBuilder {
        ReaderBuilder::default()
    }

    /// Set the CSV file's schema
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set whether the CSV file has headers
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the CSV reader to infer the schema of the file
    pub fn infer_schema(mut self, max_records: Option<usize>) -> Self {
        // remove any schema that is set
        self.schema = None;
        self.max_records = max_records;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R: Read + Seek>(self, mut reader: R) -> Result<Reader<R, DefaultParser>> {
        // check if schema should be inferred
        let delimiter = self.delimiter.unwrap_or(b',');
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                let (inferred_schema, _) = infer_file_schema(
                    &mut reader,
                    delimiter,
                    self.max_records,
                    self.has_header,
                    &infer,
                )?;

                inferred_schema
            }
        };
        Ok(Reader::from_reader(
            reader,
            schema,
            self.has_header,
            self.delimiter,
            self.batch_size,
            None,
            self.projection.clone(),
            DefaultParser::default(),
        ))
    }
}
