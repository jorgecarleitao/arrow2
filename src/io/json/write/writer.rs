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

//! # JSON Writer
//!
//! This JSON writer converts Arrow [`RecordBatch`]es into arrays of
//! JSON objects or JSON formatted byte streams.
//!
//! ## Writing JSON Objects
//!
//! To serialize [`RecordBatch`]es into array of
//! [JSON](https://docs.serde.rs/serde_json/) objects, use
//! [`write_record_batches`]:
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow2::array::Int32Array;
//! use arrow2::datatypes::{DataType, Field, Schema};
//! use arrow2::io::json;
//! use arrow2::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from_slice(&[1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! let json_rows = json::write_record_batches(&[batch]);
//! assert_eq!(
//!     serde_json::Value::Object(json_rows[1].clone()),
//!     serde_json::json!({"a": 2}),
//! );
//! ```
//!
//! ## Writing JSON formatted byte streams
//!
//! To serialize [`RecordBatch`]es into line-delimited JSON bytes, use
//! [`LineDelimitedWriter`]:
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow2::array::Int32Array;
//! use arrow2::datatypes::{DataType, Field, Schema};
//! use arrow2::io::json;
//! use arrow2::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from_slice(&[1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! // Write the record batch out as JSON
//! let buf = Vec::new();
//! let mut writer = json::LineDelimitedWriter::new(buf);
//! writer.write_batches(&vec![batch]).unwrap();
//! writer.finish().unwrap();
//!
//! // Get the underlying buffer back,
//! let buf = writer.into_inner();
//! assert_eq!(r#"{"a":1}
//! {"a":2}
//! {"a":3}
//!"#, String::from_utf8(buf).unwrap())
//! ```
//!
//! To serialize [`RecordBatch`]es into a well formed JSON array, use
//! [`ArrayWriter`]:
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow2::array::Int32Array;
//! use arrow2::datatypes::{DataType, Field, Schema};
//! use arrow2::io::json;
//! use arrow2::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from_slice(&[1, 2, 3]);
//! let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
//!
//! // Write the record batch out as a JSON array
//! let buf = Vec::new();
//! let mut writer = json::ArrayWriter::new(buf);
//! writer.write_batches(&vec![batch]).unwrap();
//! writer.finish().unwrap();
//!
//! // Get the underlying buffer back,
//! let buf = writer.into_inner();
//! assert_eq!(r#"[{"a":1},{"a":2},{"a":3}]"#, String::from_utf8(buf).unwrap())
//! ```

use std::{fmt::Debug, io::Write};

use serde_json::Value;

use crate::error::Result;
use crate::record_batch::RecordBatch;

use super::write_record_batches;

/// This trait defines how to format a sequence of JSON objects to a
/// byte stream.
pub trait JsonFormat: Debug + Default {
    #[inline]
    /// write any bytes needed at the start of the file to the writer
    fn start_stream<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the start of each row
    fn start_row<W: Write>(&self, _writer: &mut W, _is_first_row: bool) -> Result<()> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the end of each row
    fn end_row<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }

    /// write any bytes needed for the start of each row
    fn end_stream<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }
}

/// Produces JSON output with one record per line. For example
///
/// ```json
/// {"foo":1}
/// {"bar":1}
///
/// ```
#[derive(Debug, Default)]
pub struct LineDelimited {}

impl JsonFormat for LineDelimited {
    fn end_row<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// Produces JSON output as a single JSON array. For example
///
/// ```json
/// [{"foo":1},{"bar":1}]
/// ```
#[derive(Debug, Default)]
pub struct JsonArray {}

impl JsonFormat for JsonArray {
    fn start_stream<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"[")?;
        Ok(())
    }

    fn start_row<W: Write>(&self, writer: &mut W, is_first_row: bool) -> Result<()> {
        if !is_first_row {
            writer.write_all(b",")?;
        }
        Ok(())
    }

    fn end_stream<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"]")?;
        Ok(())
    }
}

/// A JSON writer which serializes [`RecordBatch`]es to newline delimited JSON objects
pub type LineDelimitedWriter<W> = Writer<W, LineDelimited>;

/// A JSON writer which serializes [`RecordBatch`]es to JSON arrays
pub type ArrayWriter<W> = Writer<W, JsonArray>;

/// A JSON writer which serializes [`RecordBatch`]es to a stream of
/// `u8` encoded JSON objects. See the module level documentation for
/// detailed usage and examples. The specific format of the stream is
/// controlled by the [`JsonFormat`] type parameter.
#[derive(Debug)]
pub struct Writer<W, F>
where
    W: Write,
    F: JsonFormat,
{
    /// Underlying writer to use to write bytes
    writer: W,

    /// Has the writer output any records yet?
    started: bool,

    /// Is the writer finished?
    finished: bool,

    /// Determines how the byte stream is formatted
    format: F,
}

impl<W, F> Writer<W, F>
where
    W: Write,
    F: JsonFormat,
{
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            started: false,
            finished: false,
            format: F::default(),
        }
    }

    /// Write a single JSON row to the output writer
    pub fn write_row(&mut self, row: &Value) -> Result<()> {
        let is_first_row = !self.started;
        if !self.started {
            self.format.start_stream(&mut self.writer)?;
            self.started = true;
        }

        self.format.start_row(&mut self.writer, is_first_row)?;
        self.writer.write_all(&serde_json::to_vec(row)?)?;
        self.format.end_row(&mut self.writer)?;
        Ok(())
    }

    /// Convert the [`RecordBatch`] into JSON rows, and write them to the output
    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for row in write_record_batches(batches) {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced. (e.g. producing the final `']'` if writing
    /// arrays.
    pub fn finish(&mut self) -> Result<()> {
        if self.started && !self.finished {
            self.format.end_stream(&mut self.writer)?;
            self.finished = true;
        }
        Ok(())
    }

    /// Unwraps this `Writer<W>`, returning the underlying writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}
