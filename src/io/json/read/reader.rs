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

use std::io::{BufReader, Read, Seek};
use std::sync::Arc;

use serde_json::Value;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use super::{deserialize::read, infer_json_schema_from_seekable, util::ValueIter};

#[derive(Debug)]
struct Decoder {
    /// Explicit schema for the JSON file
    schema: Arc<Schema>,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// Batch size (number of records to load each time)
    batch_size: usize,
}

impl Decoder {
    /// Create a new JSON decoder from any value that implements the `Iterator<Item=Result<Value>>`
    /// trait.
    pub fn new(schema: Arc<Schema>, batch_size: usize, projection: Option<Vec<String>>) -> Self {
        let schema = match &projection {
            Some(projection) => {
                let fields = schema.fields();
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
            None => schema,
        };

        Self {
            schema,
            projection,
            batch_size,
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
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
                    _ => Err(ArrowError::Other(format!(
                        "Row needs to be of type object, got: {:?}",
                        v
                    ))),
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
                .flatten()
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

/// JSON Reader
///
/// This JSON reader allows JSON line-delimited files to be read into the Arrow memory
/// model. Records are loaded in batches and are then converted from row-based data to
/// columnar data.
///
/// Example:
///
/// ```
/// use std::sync::Arc;
/// use arrow2::datatypes::{DataType, Field, Schema};
/// use arrow2::io::json;
/// use std::fs::File;
/// use std::io::BufReader;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("a", DataType::Float64, false),
///     Field::new("b", DataType::Float64, false),
///     Field::new("c", DataType::Float64, false),
/// ]));
///
/// let file = File::open("test/data/basic.json").unwrap();
///
/// let mut json = json::Reader::new(BufReader::new(file), schema, 1024, None);
/// let batch = json.next().unwrap().unwrap();
/// ```
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
        schema: Arc<Schema>,
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
        schema: Arc<Schema>,
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
    pub fn schema(&self) -> &Arc<Schema> {
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
    schema: Option<Arc<Schema>>,
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON file's schema
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
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
