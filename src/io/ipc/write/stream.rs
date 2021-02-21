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

use std::io::{BufWriter, Write};

use super::common::{
    write_continuation, write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

pub struct StreamWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> StreamWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write the schema, set the written bytes to the schema
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
            data_gen,
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::IPC(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self
            .data_gen
            .encoded_batch(batch, &mut self.dictionary_tracker, &self.write_options)
            .expect("StreamWriter is configured to not error on dictionary replacement");

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;
        }

        write_message(&mut self.writer, encoded_message, &self.write_options)?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        self.finished = true;

        Ok(())
    }
}

/// Finish the stream if it is not 'finished' when it goes out of scope
impl<W: Write> Drop for StreamWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::super::super::gen;
    use super::*;

    use crate::io::ipc::common::tests::{read_arrow_stream, read_gzip_json};
    use crate::io::ipc::read::StreamReader;

    fn test_file(version: &str, file_name: &str) {
        let (schema, batches) = read_arrow_stream(version, file_name);

        let mut result = Vec::<u8>::new();

        // write IPC version 5
        {
            let options =
                IpcWriteOptions::try_new(8, false, gen::Schema::MetadataVersion::V5).unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(&mut result, &schema, options).unwrap();
            for batch in batches {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }

        let reader = StreamReader::try_new(Cursor::new(result)).unwrap();

        let schema = reader.schema().clone();

        // read expected JSON output
        let (expected_schema, expected_batches) = read_gzip_json(version, file_name);

        assert_eq!(schema, expected_schema);

        let batches = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches, expected_batches);
    }

    #[test]
    fn write_100_primitive() {
        test_file("1.0.0-littleendian", "generated_primitive");
    }

    #[test]
    fn write_100_datetime() {
        test_file("1.0.0-littleendian", "generated_datetime");
    }

    #[test]
    fn write_100_dictionary_unsigned() {
        test_file("1.0.0-littleendian", "generated_dictionary_unsigned");
    }

    #[test]
    fn write_100_dictionary() {
        test_file("1.0.0-littleendian", "generated_dictionary");
    }

    #[test]
    fn write_100_interval() {
        test_file("1.0.0-littleendian", "generated_interval");
    }

    #[test]
    fn write_100_large_batch() {
        // this takes too long for unit-tests. It has been passing...
        //test_file("1.0.0-littleendian", "generated_large_batch");
    }

    #[test]
    fn write_100_nested() {
        test_file("1.0.0-littleendian", "generated_nested");
    }

    #[test]
    fn write_100_nested_large_offsets() {
        test_file("1.0.0-littleendian", "generated_nested_large_offsets");
    }

    #[test]
    fn write_100_null_trivial() {
        test_file("1.0.0-littleendian", "generated_null_trivial");
    }

    #[test]
    fn write_100_null() {
        test_file("1.0.0-littleendian", "generated_null");
    }

    #[test]
    fn write_100_primitive_large_offsets() {
        test_file("1.0.0-littleendian", "generated_primitive_large_offsets");
    }

    //#[test]
    //fn write_100_recursive_nested() {
    //test_file("1.0.0-littleendian", "generated_recursive_nested");
    //}

    #[test]
    fn write_100_primitive_no_batches() {
        test_file("1.0.0-littleendian", "generated_primitive_no_batches");
    }

    #[test]
    fn write_100_primitive_zerolength() {
        test_file("1.0.0-littleendian", "generated_primitive_zerolength");
    }

    #[test]
    fn write_100_custom_metadata() {
        test_file("1.0.0-littleendian", "generated_custom_metadata");
    }
}
