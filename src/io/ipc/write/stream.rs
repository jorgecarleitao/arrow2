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

//! Arrow IPC File and Stream Writers
//!
//! The `FileWriter` and `StreamWriter` have similar interfaces,
//! however the `FileWriter` expects a reader that supports `Seek`ing

use std::io::Write;

use super::common::{encoded_batch, DictionaryTracker, EncodedData, WriteOptions};
use super::common_sync::{write_continuation, write_message};
use super::schema_to_bytes;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

/// Arrow stream writer
///
/// The data written by this writer must be read in order. To signal that no more
/// data is arriving through the stream call [`self.finish()`](StreamWriter::finish);
///
/// For a usage walkthrough consult [this example](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow).
pub struct StreamWriter<W: Write> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: WriteOptions,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,
}

impl<W: Write> StreamWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(mut writer: W, schema: &Schema, write_options: WriteOptions) -> Result<Self> {
        // write the schema, set the written bytes to the schema
        let encoded_message = EncodedData {
            ipc_message: schema_to_bytes(schema),
            arrow_data: vec![],
        };
        write_message(&mut writer, encoded_message)?;
        Ok(Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::Ipc(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) =
            encoded_batch(batch, &mut self.dictionary_tracker, &self.write_options)?;

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary)?;
        }

        write_message(&mut self.writer, encoded_message)?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, 0)?;

        self.finished = true;

        Ok(())
    }

    /// Consumes itself, returning the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}
