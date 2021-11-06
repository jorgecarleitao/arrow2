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
//! Common utilities used to write to Arrow's IPC format.

use std::{collections::HashMap, sync::Arc};

use arrow_format::ipc;
use arrow_format::ipc::flatbuffers::FlatBufferBuilder;
use arrow_format::ipc::Message::CompressionType;

use crate::array::Array;
use crate::error::{ArrowError, Result};
use crate::io::ipc::endianess::is_native_little_endian;
use crate::record_batch::RecordBatch;
use crate::{array::DictionaryArray, datatypes::*};

use super::{write, write_dictionary};

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    /// LZ4 (framed)
    LZ4,
    /// ZSTD
    ZSTD,
}

/// Options declaring the behaviour of writing to IPC
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriteOptions {
    /// Whether the buffers should be compressed and which codec to use.
    /// Note: to use compression the crate must be compiled with feature `io_ipc_compression`.
    pub compression: Option<Compression>,
}

pub fn encoded_batch(
    batch: &RecordBatch,
    dictionary_tracker: &mut DictionaryTracker,
    options: &WriteOptions,
) -> Result<(Vec<EncodedData>, EncodedData)> {
    // TODO: handle nested dictionaries
    let schema = batch.schema();
    let mut encoded_dictionaries = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        if let DataType::Dictionary(_key_type, _value_type) = column.data_type() {
            let dict_id = field
                .dict_id()
                .expect("All Dictionary types have `dict_id`");

            let emit = dictionary_tracker.insert(dict_id, column)?;

            if emit {
                encoded_dictionaries.push(dictionary_batch_to_bytes(
                    dict_id,
                    column.as_ref(),
                    options,
                    is_native_little_endian(),
                ));
            }
        }
    }

    let encoded_message = record_batch_to_bytes(batch, options);

    Ok((encoded_dictionaries, encoded_message))
}

/// Write a `RecordBatch` into two sets of bytes, one for the header (ipc::Schema::Message) and the
/// other for the batch's data
fn record_batch_to_bytes(batch: &RecordBatch, options: &WriteOptions) -> EncodedData {
    let mut fbb = FlatBufferBuilder::new();

    let mut nodes: Vec<ipc::Message::FieldNode> = vec![];
    let mut buffers: Vec<ipc::Schema::Buffer> = vec![];
    let mut arrow_data: Vec<u8> = vec![];
    let mut offset = 0;
    for array in batch.columns() {
        write(
            array.as_ref(),
            &mut buffers,
            &mut arrow_data,
            &mut nodes,
            &mut offset,
            is_native_little_endian(),
            options.compression,
        )
    }

    // write data
    let buffers = fbb.create_vector(&buffers);
    let nodes = fbb.create_vector(&nodes);

    let compression = if let Some(compression) = options.compression {
        let compression = match compression {
            Compression::LZ4 => CompressionType::LZ4_FRAME,
            Compression::ZSTD => CompressionType::ZSTD,
        };
        let mut compression_builder = ipc::Message::BodyCompressionBuilder::new(&mut fbb);
        compression_builder.add_codec(compression);
        Some(compression_builder.finish())
    } else {
        None
    };

    let root = {
        let mut batch_builder = ipc::Message::RecordBatchBuilder::new(&mut fbb);
        batch_builder.add_length(batch.num_rows() as i64);
        batch_builder.add_nodes(nodes);
        batch_builder.add_buffers(buffers);
        if let Some(compression) = compression {
            batch_builder.add_compression(compression)
        }
        let b = batch_builder.finish();
        b.as_union_value()
    };
    // create an ipc::Schema::Message
    let mut message = ipc::Message::MessageBuilder::new(&mut fbb);
    message.add_version(ipc::Schema::MetadataVersion::V5);
    message.add_header_type(ipc::Message::MessageHeader::RecordBatch);
    message.add_bodyLength(arrow_data.len() as i64);
    message.add_header(root);
    let root = message.finish();
    fbb.finish(root, None);
    let finished_data = fbb.finished_data();

    EncodedData {
        ipc_message: finished_data.to_vec(),
        arrow_data,
    }
}

/// Write dictionary values into two sets of bytes, one for the header (ipc::Schema::Message) and the
/// other for the data
fn dictionary_batch_to_bytes(
    dict_id: i64,
    array: &dyn Array,
    options: &WriteOptions,
    is_little_endian: bool,
) -> EncodedData {
    let mut fbb = FlatBufferBuilder::new();

    let mut nodes: Vec<ipc::Message::FieldNode> = vec![];
    let mut buffers: Vec<ipc::Schema::Buffer> = vec![];
    let mut arrow_data: Vec<u8> = vec![];

    let length = write_dictionary(
        array,
        &mut buffers,
        &mut arrow_data,
        &mut nodes,
        &mut 0,
        is_little_endian,
        options.compression,
        false,
    );

    // write data
    let buffers = fbb.create_vector(&buffers);
    let nodes = fbb.create_vector(&nodes);

    let compression = if let Some(compression) = options.compression {
        let compression = match compression {
            Compression::LZ4 => CompressionType::LZ4_FRAME,
            Compression::ZSTD => CompressionType::ZSTD,
        };
        let mut compression_builder = ipc::Message::BodyCompressionBuilder::new(&mut fbb);
        compression_builder.add_codec(compression);
        Some(compression_builder.finish())
    } else {
        None
    };

    let root = {
        let mut batch_builder = ipc::Message::RecordBatchBuilder::new(&mut fbb);
        batch_builder.add_length(length as i64);
        batch_builder.add_nodes(nodes);
        batch_builder.add_buffers(buffers);
        if let Some(compression) = compression {
            batch_builder.add_compression(compression)
        }
        batch_builder.finish()
    };

    let root = {
        let mut batch_builder = ipc::Message::DictionaryBatchBuilder::new(&mut fbb);
        batch_builder.add_id(dict_id);
        batch_builder.add_data(root);
        batch_builder.finish().as_union_value()
    };

    let root = {
        let mut message_builder = ipc::Message::MessageBuilder::new(&mut fbb);
        message_builder.add_version(ipc::Schema::MetadataVersion::V5);
        message_builder.add_header_type(ipc::Message::MessageHeader::DictionaryBatch);
        message_builder.add_bodyLength(arrow_data.len() as i64);
        message_builder.add_header(root);
        message_builder.finish()
    };

    fbb.finish(root, None);
    let finished_data = fbb.finished_data();

    EncodedData {
        ipc_message: finished_data.to_vec(),
        arrow_data,
    }
}

/// Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
/// multiple times. Can optionally error if an update to an existing dictionary is attempted, which
/// isn't allowed in the `FileWriter`.
pub struct DictionaryTracker {
    written: HashMap<i64, Arc<dyn Array>>,
    error_on_replacement: bool,
}

impl DictionaryTracker {
    pub fn new(error_on_replacement: bool) -> Self {
        Self {
            written: HashMap::new(),
            error_on_replacement,
        }
    }

    /// Keep track of the dictionary with the given ID and values. Behavior:
    ///
    /// * If this ID has been written already and has the same data, return `Ok(false)` to indicate
    ///   that the dictionary was not actually inserted (because it's already been seen).
    /// * If this ID has been written already but with different data, and this tracker is
    ///   configured to return an error, return an error.
    /// * If the tracker has not been configured to error on replacement or this dictionary
    ///   has never been seen before, return `Ok(true)` to indicate that the dictionary was just
    ///   inserted.
    pub fn insert(&mut self, dict_id: i64, array: &Arc<dyn Array>) -> Result<bool> {
        let values = match array.data_type() {
            DataType::Dictionary(key_type, _) => {
                with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$T>>()
                        .unwrap();
                    array.values()
                })
            }
            _ => unreachable!(),
        };

        // If a dictionary with this id was already emitted, check if it was the same.
        if let Some(last) = self.written.get(&dict_id) {
            if last.as_ref() == values.as_ref() {
                // Same dictionary values => no need to emit it again
                return Ok(false);
            } else if self.error_on_replacement {
                return Err(ArrowError::InvalidArgumentError(
                    "Dictionary replacement detected when writing IPC file format. \
                     Arrow IPC files only support a single dictionary for a given field \
                     across all batches."
                        .to_string(),
                ));
            }
        };

        self.written.insert(dict_id, values.clone());
        Ok(true)
    }
}

/// Stores the encoded data, which is an ipc::Schema::Message, and optional Arrow data
pub struct EncodedData {
    /// An encoded ipc::Schema::Message
    pub ipc_message: Vec<u8>,
    /// Arrow buffers to be written, should be an empty vec for schema messages
    pub arrow_data: Vec<u8>,
}

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
pub(crate) fn pad_to_8(len: usize) -> usize {
    (((len + 7) & !7) - len) as usize
}
