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

use super::super::ARROW_MAGIC;
use super::{
    super::{convert, gen},
    common::{
        write_continuation, write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
    },
};
use flatbuffers::FlatBufferBuilder;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

pub struct FileWriter<'a, W: Write> {
    /// The object to write to
    writer: &'a mut W,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// The number of bytes between each block of bytes, as an offset for random access
    block_offsets: usize,
    /// Dictionary blocks that will be written as part of the IPC footer
    dictionary_blocks: Vec<gen::File::Block>,
    /// Record blocks that will be written as part of the IPC footer
    record_blocks: Vec<gen::File::Block>,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<'a, W: Write> FileWriter<'a, W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: &'a mut W, schema: &Schema) -> Result<Self> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try create a new writer with IpcWriteOptions
    pub fn try_new_with_options(
        writer: &'a mut W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let data_gen = IpcDataGenerator::default();
        // write magic to header
        writer.write_all(&ARROW_MAGIC[..])?;
        // create an 8-byte boundary after the header
        writer.write_all(&[0, 0])?;
        // write the schema, set the written bytes to the schema + header
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        let (meta, data) = write_message(writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            block_offsets: meta + data + 8,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker: DictionaryTracker::new(true),
            data_gen,
        })
    }

    /// Write a record batch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::IPC(
                "Cannot write record batch to file writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) =
                write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;

            let block = gen::File::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        let (meta, data) = write_message(&mut self.writer, encoded_message, &self.write_options)?;
        // add a record block for the footer
        let block = gen::File::Block::new(
            self.block_offsets as i64,
            meta as i32, // TODO: is this still applicable?
            data as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta + data;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        // write EOS
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);
        let schema = convert::schema_to_fb_offset(&mut fbb, &self.schema);

        let root = {
            let mut footer_builder = gen::File::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(self.write_options.metadata_version);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        self.writer.write_all(footer_data)?;
        self.writer
            .write_all(&(footer_data.len() as i32).to_le_bytes())?;
        self.writer.write_all(&ARROW_MAGIC)?;
        self.writer.flush()?;
        self.finished = true;

        Ok(())
    }
}

/// Finish the file if it is not 'finished' when it goes out of scope
impl<'a, W: Write> Drop for FileWriter<'a, W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use crate::io::ipc::common::tests::{read_arrow_file, read_gzip_json};
    use crate::io::ipc::read::FileReader;

    fn test_file(version: &str, file_name: &str) {
        let (schema, batches) = read_arrow_file(version, file_name);

        let mut result = Vec::<u8>::new();

        // write IPC version 5
        {
            let options =
                IpcWriteOptions::try_new(8, false, gen::Schema::MetadataVersion::V5).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(&mut result, &schema, options).unwrap();
            for batch in batches {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }

        let reader = FileReader::try_new(Cursor::new(result)).unwrap();

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

    #[test]
    fn write_100_decimal() {
        test_file("1.0.0-littleendian", "generated_decimal");
    }
}
