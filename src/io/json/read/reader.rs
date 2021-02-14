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

//! JSON Reader
//!
//! This JSON reader allows JSON line-delimited files to be read into the Arrow memory
//! model. Records are loaded in batches and are then converted from row-based data to
//! columnar data.
//!
//! Example:
//!
//! ```
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use std::fs::File;
//! use std::io::BufReader;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("a", DataType::Float64, false),
//!     Field::new("b", DataType::Float64, false),
//!     Field::new("c", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/basic.json").unwrap();
//!
//! let mut json = json::Reader::new(BufReader::new(file), Arc::new(schema), 1024, None);
//! let batch = json.next().unwrap().unwrap();
//! ```

use std::io::{BufReader, Read, Seek};
use std::sync::Arc;

use serde_json::Value;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use super::{deserialize::read, infer_json_schema_from_seekable, util::ValueIter};

type SchemaRef = Arc<Schema>;

/// JSON values to Arrow record batch decoder. Decoder's next_batch method takes a JSON Value
/// iterator as input and outputs Arrow record batch.
///
/// # Examples
/// ```
/// use arrow::json::reader::{Decoder, ValueIter, infer_json_schema};
/// use std::fs::File;
/// use std::io::{BufReader, Seek, SeekFrom};
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// let batch_size = 1024;
/// let decoder = Decoder::new(inferred_schema, batch_size, None);
///
/// // seek back to start so that the original file is usable again
/// reader.seek(SeekFrom::Start(0)).unwrap();
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// let batch = decoder.next_batch(&mut value_reader).unwrap().unwrap();
/// assert_eq!(4, batch.num_rows());
/// assert_eq!(4, batch.num_columns());
/// ```
#[derive(Debug)]
struct Decoder {
    /// Explicit schema for the JSON file
    schema: SchemaRef,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// Batch size (number of records to load each time)
    batch_size: usize,
}

impl Decoder {
    /// Create a new JSON decoder from any value that implements the `Iterator<Item=Result<Value>>`
    /// trait.
    pub fn new(schema: SchemaRef, batch_size: usize, projection: Option<Vec<String>>) -> Self {
        Self {
            schema,
            projection,
            batch_size,
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> = fields
                    .iter()
                    .filter_map(|field| {
                        if projection.contains(field.name()) {
                            Some(field.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    /// Read the next batch of records
    pub fn next_batch<I>(&self, value_iter: &mut I) -> Result<Option<RecordBatch>>
    where
        I: Iterator<Item = Result<Value>>,
    {
        let rows = value_iter
            .take(self.batch_size)
            .map(|value| {
                let v = value?;
                match v {
                    Value::Object(_) => Ok(v),
                    _ => {
                        return Err(ArrowError::JsonError(format!(
                            "Row needs to be of type object, got: {:?}",
                            v
                        )));
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = rows.iter().collect::<Vec<_>>();

        if rows.is_empty() {
            // reached end of file
            return Ok(None);
        }

        let projection = self.projection.clone().unwrap_or_else(Vec::new);

        let projected_fields = if projection.is_empty() {
            self.schema.fields().to_vec()
        } else {
            projection
                .iter()
                .map(|name| self.schema.column_with_name(name).map(|x| x.1.clone()))
                .filter_map(|c| c)
                .collect()
        };

        let data_type = DataType::Struct(projected_fields.clone());
        let array = read(&rows, data_type);
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let arrays = array.values().to_vec();

        let projected_schema = Arc::new(Schema::new(projected_fields));

        RecordBatch::try_new(projected_schema, arrays).map(Some)
    }
}

/// JSON file reader
#[derive(Debug)]
pub struct Reader<R: Read> {
    reader: BufReader<R>,
    /// JSON value decoder
    decoder: Decoder,
}

impl<R: Read> Reader<R> {
    /// Create a new JSON Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn new(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self::from_buf_reader(BufReader::new(reader), schema, batch_size, projection)
    }

    /// Create a new JSON Reader from a `BufReader<R: Read>`
    ///
    /// To customize the schema, such as to enable schema inference, use `ReaderBuilder`
    pub fn from_buf_reader(
        reader: BufReader<R>,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            reader,
            decoder: Decoder::new(schema, batch_size, projection),
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.decoder
            .next_batch(&mut ValueIter::new(&mut self.reader, None))
    }
}

/// JSON file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the JSON file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the JSON structure.
    schema: Option<SchemaRef>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            max_records: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring JSON parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate arrow;
    ///
    /// use arrow::json;
    /// use std::fs::File;
    ///
    /// fn example() -> json::Reader<File> {
    ///     let file = File::open("test/data/basic.json").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = json::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build::<File>(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the JSON reader to infer the schema of the file
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
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R>(self, source: R) -> Result<Reader<R>>
    where
        R: Read + Seek,
    {
        let mut buf_reader = BufReader::new(source);

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => Arc::new(infer_json_schema_from_seekable(
                &mut buf_reader,
                self.max_records,
            )?),
        };

        Ok(Reader::from_buf_reader(
            buf_reader,
            schema,
            self.batch_size,
            self.projection,
        ))
    }
}

#[cfg(test)]
mod tests {
    use flate2::read::GzDecoder;
    use std::iter::FromIterator;

    use crate::{
        buffer::Bitmap,
        datatypes::DataType::{Dictionary, List},
        io::json::read::infer_json_schema,
    };

    use super::*;
    use std::{fs::File, io::SeekFrom};

    #[test]
    fn test_json_basic() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(1, b.0);
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(2, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(3, d.0);
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        assert!(2.0 - bb.value(0) < f64::EPSILON);
        assert!(-3.5 - bb.value(1) < f64::EPSILON);
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(false, cc.value(0));
        assert_eq!(true, cc.value(10));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("4", dd.value(0));
        assert_eq!("text", dd.value(8));
    }

    #[test]
    fn test_json_basic_with_nulls() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        assert_eq!(true, bb.is_valid(0));
        assert_eq!(false, bb.is_valid(2));
        assert_eq!(false, bb.is_valid(11));
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(true, cc.is_valid(0));
        assert_eq!(false, cc.is_valid(4));
        assert_eq!(false, cc.is_valid(11));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(false, dd.is_valid(0));
        assert_eq!(true, dd.is_valid(1));
        assert_eq!(false, dd.is_valid(4));
        assert_eq!(false, dd.is_valid(11));
    }

    #[test]
    fn test_json_basic_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::Utf8, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema.clone()),
            1024,
            None,
        );
        let reader_schema = reader.schema();
        assert_eq!(reader_schema, Arc::new(schema));
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int32, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float32, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        // test that a 64bit value is returned as null due to overflowing
        assert_eq!(false, aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert!(2.0 - bb.value(0) < f32::EPSILON);
        assert!(-3.5 - bb.value(1) < f32::EPSILON);
    }

    #[test]
    fn test_json_basic_schema_projection() {
        // We test implicit and explicit projection:
        // Implicit: omitting fields from a schema
        // Explicit: supplying a vec of fields to take
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema),
            1024,
            Some(vec!["a".to_string(), "c".to_string()]),
        );
        let reader_schema = reader.schema();
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Boolean, false),
        ]));
        assert_eq!(reader_schema, expected_schema);

        let batch = reader.next().unwrap().unwrap();

        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.schema().fields().len());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();
        assert_eq!(reader_schema, schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int32, a.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(1, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
    }

    #[test]
    fn test_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            c.1.data_type()
        );
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        let bb = bb.values();
        let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(9, bb.len());
        assert!(2.0 - bb.value(0) < f64::EPSILON);
        assert!(-6.1 - bb.value(5) < f64::EPSILON);
        assert_eq!(false, bb.is_valid(7));

        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        let cc = cc.values();
        let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(6, cc.len());
        assert_eq!(false, cc.value(0));
        assert_eq!(false, cc.value(4));
        assert_eq!(false, cc.is_valid(5));
    }

    #[test]
    fn test_invalid_json_infer_schema() {
        let re = infer_json_schema_from_seekable(
            &mut BufReader::new(File::open("test/data/uk_cities_with_headers.csv").unwrap()),
            None,
        );
        assert_eq!(
            re.err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_invalid_json_read_record() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
            true,
        )]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/uk_cities_with_headers.csv").unwrap())
            .unwrap();
        assert_eq!(
            reader.next().err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_mixed_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/mixed_arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let schema = Arc::new(infer_json_schema(&mut reader, None).unwrap());
        file.seek(SeekFrom::Start(0)).unwrap();

        let reader = BufReader::new(GzDecoder::new(&file));
        let mut reader = Reader::from_buf_reader(reader, schema, 64, None);
        let batch_gz = reader.next().unwrap().unwrap();

        for batch in vec![batch, batch_gz] {
            assert_eq!(4, batch.num_columns());
            assert_eq!(4, batch.num_rows());

            let schema = batch.schema();

            let a = schema.column_with_name("a").unwrap();
            assert_eq!(&DataType::Int64, a.1.data_type());
            let b = schema.column_with_name("b").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                b.1.data_type()
            );
            let c = schema.column_with_name("c").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                c.1.data_type()
            );
            let d = schema.column_with_name("d").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                d.1.data_type()
            );

            let bb = batch
                .column(b.0)
                .as_any()
                .downcast_ref::<ListArray<i32>>()
                .unwrap();
            let bb = bb.values();
            let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(9, bb.len());
            assert!(-6.1 - bb.value(8) < f64::EPSILON);

            let cc = batch
                .column(c.0)
                .as_any()
                .downcast_ref::<ListArray<i32>>()
                .unwrap();
            let cc = cc.values();
            let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
            let cc_expected = BooleanArray::from(vec![Some(false), Some(true), Some(false), None]);
            assert_eq!(cc, &cc_expected);

            let dd = batch
                .column(d.0)
                .as_any()
                .downcast_ref::<ListArray<i32>>()
                .unwrap();
            let dd = dd.values();
            let dd = dd.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                dd,
                &StringArray::from_slice(&["1", "false", "array", "2.4"])
            );
        }
    }

    #[test]
    fn test_nested_struct_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/nested_structs.json").unwrap())
            .unwrap();

        // build expected output
        let d = StringArray::from(&vec![Some("text"), None, Some("text"), None]);
        let c = StructArray::from_data(
            vec![c_field.clone()],
            vec![Arc::new(d)],
            Some(Bitmap::from_iter(vec![true, true, true, false])),
        );

        let b = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let expected = StructArray::from_data(
            vec![Field::new("b", DataType::Boolean, true), c_field],
            vec![Arc::new(b), Arc::new(c)],
            Some(Bitmap::from_iter(vec![true, true, true, false])),
        );

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert_eq!(expected, read.as_ref());
    }

    /*
    #[test]
    fn test_nested_list_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_struct_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let a_field = Field::new("a", DataType::List(Box::new(a_struct_field.clone())), true);
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let json_content = r#"
        {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
        {"a": [{"b": false, "c": null}]}
        {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
        {"a": null}
        {"a": []}
        "#;
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();

        // build expected output
        let d = StringArray::from(vec![
            Some("a_text"),
            Some("b_text"),
            None,
            Some("c_text"),
            Some("d_text"),
            None,
            None,
        ]);
        let c = ArrayDataBuilder::new(c_field.data_type().clone())
            .len(7)
            .add_child_data(d.data())
            .null_bit_buffer(Buffer::from(vec![0b00111011]))
            .build();
        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
            Some(true),
            None,
        ]);
        let a = ArrayDataBuilder::new(a_struct_field.data_type().clone())
            .len(7)
            .add_child_data(b.data())
            .add_child_data(c.clone())
            .null_bit_buffer(Buffer::from(vec![0b00111111]))
            .build();
        let a_list = ArrayDataBuilder::new(a_field.data_type().clone())
            .len(5)
            .add_buffer(Buffer::from_slice_ref(&[0i32, 2, 3, 6, 6, 6]))
            .add_child_data(a)
            .null_bit_buffer(Buffer::from(vec![0b00010111]))
            .build();
        let expected = make_array(a_list);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert_eq!(read.len(), 5);
        // compare the arrays the long way around, to better detect differences
        let read: &ListArray = read.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = expected.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            read.data().buffers()[0],
            Buffer::from_slice_ref(&[0i32, 2, 3, 6, 6, 6])
        );
        // compare list null buffers
        assert_eq!(read.data().null_buffer(), expected.data().null_buffer());
        // build struct from list
        let struct_values = read.values();
        let struct_array: &StructArray = struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let expected_struct_values = expected.values();
        let expected_struct_array = expected_struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_eq!(7, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(7, expected_struct_array.len());
        assert_eq!(1, expected_struct_array.null_count());
        // test struct's nulls
        assert_eq!(
            struct_array.data().null_buffer(),
            expected_struct_array.data().null_buffer()
        );
        // test struct's fields
        let read_b = struct_array.column(0);
        assert_eq!(b.data_ref(), read_b.data_ref());
        let read_c = struct_array.column(1);
        assert_eq!(&c, read_c.data_ref());
        let read_c: &StructArray = read_c.as_any().downcast_ref::<StructArray>().unwrap();
        let read_d = read_c.column(0);
        assert_eq!(d.data_ref(), read_d.data_ref());

        assert_eq!(read.data_ref(), expected.data_ref());
    }

    #[test]
    fn test_dictionary_from_json_basic_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            d.1.data_type()
        );

        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .unwrap();
        assert_eq!(false, dd.is_valid(0));
        assert_eq!(true, dd.is_valid(1));
        assert_eq!(true, dd.is_valid(2));
        assert_eq!(false, dd.is_valid(11));

        assert_eq!(
            dd.keys(),
            &Int16Array::from(vec![
                None,
                Some(0),
                Some(1),
                Some(0),
                None,
                None,
                Some(0),
                None,
                Some(1),
                Some(0),
                Some(0),
                None
            ])
        );
    }

    #[test]
    fn test_dictionary_from_json_int8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_skip_empty_lines() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let json_content = "
        {\"a\": 1}

        {\"a\": 2}

        {\"a\": 3}";
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let c = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, c.1.data_type());
    }

    #[test]
    fn test_row_type_validation() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let json_content = "
        [1, \"hello\"]
        \"world\"";
        let re = builder.build(Cursor::new(json_content));
        assert_eq!(
            re.err().unwrap().to_string(),
            r#"Json error: Expected JSON record to be an object, found Array([Number(1), String("hello")])"#,
        );
    }

    #[test]
    fn test_list_of_string_dictionary_from_json() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/list_string_dict_nested.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(6, evs_list.len());
        assert_eq!(true, evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_list_of_string_dictionary_from_json_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/list_string_dict_nested_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(8, evs_list.len());
        assert_eq!(true, evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, evs_list.null_count());
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_dictionary_from_json_uint8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_with_multiple_batches() {
        let builder = ReaderBuilder::new()
            .infer_schema(Some(4))
            .with_batch_size(5);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();

        let mut num_records = Vec::new();
        while let Some(rb) = reader.next().unwrap() {
            num_records.push(rb.num_rows());
        }

        assert_eq!(vec![5, 5, 2], num_records);
    }

    #[test]
    fn test_json_infer_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "c",
                DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "d",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        let mut reader = BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
        let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, Arc::new(schema.clone()));

        let file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let inferred_schema = infer_json_schema(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, Arc::new(schema));
    }

    #[test]
    fn test_timestamp_from_json_seconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Second, None),
            a.1.data_type()
        );

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_timestamp_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            a.1.data_type()
        );

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_date_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new("a", DataType::Date64, true)]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Date64, a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_time_from_json_nanoseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Time64(TimeUnit::Nanosecond), a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }
    */
}
