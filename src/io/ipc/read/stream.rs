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

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::super::CONTINUATION_MARKER;
use super::super::{convert, gen};
use super::common::*;

type ArrayRef = Arc<dyn Array>;

#[derive(Debug)]
pub struct StreamMetadata {
    /// The schema that is read from the stream's first message
    schema: Arc<Schema>,

    /// Whether the incoming stream is little-endian
    is_little_endian: bool,
}

/// Reads the metadata of the stream
pub fn read_stream_metadata<R: Read>(reader: &mut R) -> Result<StreamMetadata> {
    // determine metadata length
    let mut meta_size: [u8; 4] = [0; 4];
    reader.read_exact(&mut meta_size)?;
    let meta_len = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_size == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_size)?;
        }
        i32::from_le_bytes(meta_size)
    };

    let mut meta_buffer = vec![0; meta_len as usize];
    reader.read_exact(&mut meta_buffer)?;

    let message = gen::Message::root_as_message(meta_buffer.as_slice())
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;
    // message header is a Schema, so read it
    let ipc_schema: gen::Schema::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::Ipc("Unable to read IPC message as schema".to_string()))?;
    let (schema, is_little_endian) = convert::fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);

    Ok(StreamMetadata {
        schema,
        is_little_endian,
    })
}

/// Reads the next item
pub fn read_next<R: Read>(
    reader: &mut R,
    metadata: &StreamMetadata,
    dictionaries_by_field: &mut Vec<Option<ArrayRef>>,
) -> Result<Option<RecordBatch>> {
    // determine metadata length
    let mut meta_size: [u8; 4] = [0; 4];

    match reader.read_exact(&mut meta_size) {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(None)
            } else {
                Err(ArrowError::from(e))
            };
        }
    }

    let meta_len = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_size == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_size)?;
        }
        i32::from_le_bytes(meta_size)
    };

    if meta_len == 0 {
        // the stream has ended, mark the reader as finished
        return Ok(None);
    }

    let mut meta_buffer = vec![0; meta_len as usize];
    reader.read_exact(&mut meta_buffer)?;

    let vecs = &meta_buffer.to_vec();
    let message = gen::Message::root_as_message(vecs)
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;

    match message.header_type() {
        gen::Message::MessageHeader::Schema => Err(ArrowError::Ipc(
            "Not expecting a schema when messages are read".to_string(),
        )),
        gen::Message::MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or_else(|| {
                ArrowError::Ipc("Unable to read IPC message as record batch".to_string())
            })?;
            // read the block that makes up the record batch into a buffer
            let mut buf = vec![0; message.bodyLength() as usize];
            reader.read_exact(&mut buf)?;

            let mut reader = std::io::Cursor::new(buf);

            read_record_batch(
                batch,
                metadata.schema.clone(),
                metadata.is_little_endian,
                &dictionaries_by_field,
                &mut reader,
                0,
            )
            .map(Some)
        }
        gen::Message::MessageHeader::DictionaryBatch => {
            let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                ArrowError::Ipc("Unable to read IPC message as dictionary batch".to_string())
            })?;
            // read the block that makes up the dictionary batch into a buffer
            let mut buf = vec![0; message.bodyLength() as usize];
            reader.read_exact(&mut buf)?;

            let mut dict_reader = std::io::Cursor::new(buf);

            read_dictionary(
                batch,
                &metadata.schema,
                metadata.is_little_endian,
                dictionaries_by_field,
                &mut dict_reader,
                0,
            )?;

            // read the next message until we encounter a RecordBatch
            read_next(reader, metadata, dictionaries_by_field)
        }
        gen::Message::MessageHeader::NONE => Ok(None),
        t => Err(ArrowError::Ipc(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}

/// Arrow Stream reader
pub struct StreamReader<R: Read> {
    reader: R,
    metadata: StreamMetadata,
    dictionaries_by_field: Vec<Option<ArrayRef>>,
    finished: bool,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        let fields = metadata.schema.fields().len();
        Self {
            reader,
            metadata,
            dictionaries_by_field: vec![None; fields],
            finished: false,
        }
    }

    /// Return the schema of the stream
    pub fn schema(&self) -> &Arc<Schema> {
        &self.metadata.schema
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>> {
        let batch = read_next(
            &mut self.reader,
            &self.metadata,
            &mut self.dictionaries_by_field,
        )?;
        if batch.is_none() {
            self.finished = false;
        }
        if self.finished {
            return Ok(None);
        }
        Ok(batch)
    }
}

impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}

impl<R: Read> RecordBatchReader for StreamReader<R> {
    fn schema(&self) -> &Schema {
        self.metadata.schema.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io::ipc::common::tests::read_gzip_json;

    use std::fs::File;

    fn test_file(version: &str, file_name: &str) -> Result<()> {
        let testdata = crate::util::test_util::arrow_test_data();
        let mut file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, file_name
        ))?;

        let metadata = read_stream_metadata(&mut file)?;
        let reader = StreamReader::new(file, metadata);

        // read expected JSON output
        let (schema, batches) = read_gzip_json(version, file_name);

        assert_eq!(&schema, reader.schema().as_ref());

        batches
            .iter()
            .zip(reader.map(|x| x.unwrap()))
            .for_each(|(lhs, rhs)| {
                assert_eq!(lhs, &rhs);
            });
        Ok(())
    }

    #[test]
    fn read_generated_100_primitive() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive")
    }

    #[test]
    fn read_generated_100_datetime() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_datetime")
    }

    #[test]
    fn read_generated_100_null_trivial() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_null_trivial")
    }

    #[test]
    fn read_generated_100_null() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_null")
    }

    #[test]
    fn read_generated_100_primitive_zerolength() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive_zerolength")
    }

    #[test]
    fn read_generated_100_primitive_primitive_no_batches() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive_no_batches")
    }

    #[test]
    fn read_generated_100_dictionary() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_dictionary")
    }

    #[test]
    fn read_generated_100_nested() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_nested")
    }

    #[test]
    fn read_generated_100_interval() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_interval")
    }

    #[test]
    fn read_generated_100_decimal() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_decimal")
    }
}
