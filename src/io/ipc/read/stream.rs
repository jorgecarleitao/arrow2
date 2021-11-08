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

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow_format::ipc;
use arrow_format::ipc::Schema::MetadataVersion;

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use super::super::convert;
use super::super::CONTINUATION_MARKER;
use super::common::*;

#[derive(Debug)]
pub struct StreamMetadata {
    /// The schema that is read from the stream's first message
    schema: Arc<Schema>,

    version: MetadataVersion,

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

    let message = ipc::Message::root_as_message(meta_buffer.as_slice())
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;
    let version = message.version();
    // message header is a Schema, so read it
    let ipc_schema: ipc::Schema::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::Ipc("Unable to read IPC message as schema".to_string()))?;
    let (schema, is_little_endian) = convert::fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);

    Ok(StreamMetadata {
        schema,
        version,
        is_little_endian,
    })
}

/// Encodes the stream's status after each read.
///
/// A stream is an iterator, and an iterator returns `Option<Item>`. The `Item`
/// type in the [`StreamReader`] case is `StreamState`, which means that an Arrow
/// stream may yield one of three values: (1) `None`, which signals that the stream
/// is done; (2) `Some(StreamState::Some(RecordBatch))`, which signals that there was
/// data waiting in the stream and we read it; and finally (3)
/// `Some(StreamState::Waiting)`, which means that the stream is still "live", it
/// just doesn't hold any data right now.
pub enum StreamState {
    /// A live stream without data
    Waiting,
    /// Next item in the stream
    Some(RecordBatch),
}

impl StreamState {
    /// Return the data inside this wrapper.
    ///
    /// # Panics
    ///
    /// If the `StreamState` was `Waiting`.
    pub fn unwrap(self) -> RecordBatch {
        if let StreamState::Some(batch) = self {
            batch
        } else {
            panic!("The batch is not available")
        }
    }
}

/// Reads the next item, yielding `None` if the stream is done,
/// and a [`StreamState`] otherwise.
pub fn read_next<R: Read>(
    reader: &mut R,
    metadata: &StreamMetadata,
    dictionaries: &mut HashMap<usize, Arc<dyn Array>>,
) -> Result<Option<StreamState>> {
    // determine metadata length
    let mut meta_size: [u8; 4] = [0; 4];

    match reader.read_exact(&mut meta_size) {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(Some(StreamState::Waiting))
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
    let message = ipc::Message::root_as_message(vecs)
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;

    match message.header_type() {
        ipc::Message::MessageHeader::Schema => Err(ArrowError::Ipc(
            "Not expecting a schema when messages are read".to_string(),
        )),
        ipc::Message::MessageHeader::RecordBatch => {
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
                None,
                metadata.is_little_endian,
                dictionaries,
                metadata.version,
                &mut reader,
                0,
            )
            .map(|x| Some(StreamState::Some(x)))
        }
        ipc::Message::MessageHeader::DictionaryBatch => {
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
                dictionaries,
                &mut dict_reader,
                0,
            )?;

            // read the next message until we encounter a RecordBatch
            read_next(reader, metadata, dictionaries)
        }
        ipc::Message::MessageHeader::NONE => Ok(Some(StreamState::Waiting)),
        t => Err(ArrowError::Ipc(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}

/// Arrow Stream reader.
///
/// An [`Iterator`] over an Arrow stream that yields a result of [`StreamState`]s.
/// This is the recommended way to read an arrow stream (by iterating over its data).
///
/// For a more thorough walkthrough consult [this example](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow).
pub struct StreamReader<R: Read> {
    reader: R,
    metadata: StreamMetadata,
    dictionaries: HashMap<usize, Arc<dyn Array>>,
    finished: bool,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        Self {
            reader,
            metadata,
            dictionaries: Default::default(),
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

    fn maybe_next(&mut self) -> Result<Option<StreamState>> {
        if self.finished {
            return Ok(None);
        }
        let batch = read_next(&mut self.reader, &self.metadata, &mut self.dictionaries)?;
        if batch.is_none() {
            self.finished = true;
        }
        Ok(batch)
    }
}

impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<StreamState>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}
