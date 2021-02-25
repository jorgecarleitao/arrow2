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

use std::io::{BufWriter, Write};

use serde_json::Value;

use crate::error::Result;
use crate::record_batch::RecordBatch;

use super::serialize::write_record_batches;

/// JSON Writer
///
/// This JSON writer allows converting Arrow record batches into array of JSON objects. It also
/// provides a Writer struct to help serialize record batches directly into line-delimited JSON
/// objects as bytes.
///
/// Serialize record batches into line-delimited JSON bytes:
///
/// ```
/// use std::sync::Arc;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::{DataType, Field, Schema};
/// use arrow2::io::json;
/// use arrow2::record_batch::RecordBatch;
///
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
/// let a = Primitive::from_slice(&[1i32, 2, 3]).to(DataType::Int32);
/// let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
///
/// let buf = Vec::new();
/// let mut writer = json::Writer::new(buf);
/// writer.write_batches(&vec![batch]).unwrap();
/// ```
#[derive(Debug)]
pub struct Writer<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self::from_buf_writer(BufWriter::new(writer))
    }

    pub fn from_buf_writer(writer: BufWriter<W>) -> Self {
        Self { writer }
    }

    pub fn write_row(&mut self, row: &Value) -> Result<()> {
        self.writer.write_all(&serde_json::to_vec(row)?)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for row in write_record_batches(batches) {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }
}
