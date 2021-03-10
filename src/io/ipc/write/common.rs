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
use std::{collections::HashMap, sync::Arc};

use super::super::CONTINUATION_MARKER;
use super::super::{convert, gen};
use super::{write, write_dictionary};
use flatbuffers::FlatBufferBuilder;

use crate::array::Array;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;
use crate::{array::DictionaryArray, datatypes::*};

/// IPC write options used to control the behaviour of the writer
#[derive(Debug)]
pub struct IpcWriteOptions {
    /// Write padding after memory buffers to this multiple of bytes.
    /// Generally 8 or 64, defaults to 8
    alignment: usize,
    /// The legacy format is for releases before 0.15.0, and uses metadata V4
    write_legacy_ipc_format: bool,
    /// The metadata version to write. The Rust IPC writer supports V4+
    ///
    /// *Default versions per crate*
    ///
    /// When creating the default IpcWriteOptions, the following metadata versions are used:
    ///
    /// version 2.0.0: V4, with legacy format enabled
    /// version 4.0.0: V5
    pub(super) metadata_version: gen::Schema::MetadataVersion,
}

impl IpcWriteOptions {
    /// Try create IpcWriteOptions, checking for incompatible settings
    pub fn try_new(
        alignment: usize,
        write_legacy_ipc_format: bool,
        metadata_version: gen::Schema::MetadataVersion,
    ) -> Result<Self> {
        if alignment == 0 || alignment % 8 != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Alignment should be greater than 0 and be a multiple of 8".to_string(),
            ));
        }
        match metadata_version {
            gen::Schema::MetadataVersion::V1
            | gen::Schema::MetadataVersion::V2
            | gen::Schema::MetadataVersion::V3 => Err(ArrowError::InvalidArgumentError(
                "Writing IPC metadata version 3 and lower not supported".to_string(),
            )),
            gen::Schema::MetadataVersion::V4 => Ok(Self {
                alignment,
                write_legacy_ipc_format,
                metadata_version,
            }),
            gen::Schema::MetadataVersion::V5 => {
                if write_legacy_ipc_format {
                    Err(ArrowError::InvalidArgumentError(
                        "Legacy IPC format only supported on metadata version 4".to_string(),
                    ))
                } else {
                    Ok(Self {
                        alignment,
                        write_legacy_ipc_format,
                        metadata_version,
                    })
                }
            }
            z => panic!("Unsupported gen::Schema::MetadataVersion {:?}", z),
        }
    }
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: 8,
            write_legacy_ipc_format: false,
            metadata_version: gen::Schema::MetadataVersion::V5,
        }
    }
}

#[derive(Debug, Default)]
pub struct IpcDataGenerator {}

impl IpcDataGenerator {
    pub fn schema_to_bytes(&self, schema: &Schema, write_options: &IpcWriteOptions) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();
        let schema = {
            let fb = convert::schema_to_fb_offset(&mut fbb, schema);
            fb.as_union_value()
        };

        let mut message = gen::Message::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(gen::Message::MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema);
        // TODO: custom metadata
        let data = message.finish();
        fbb.finish(data, None);

        let data = fbb.finished_data();
        EncodedData {
            ipc_message: data.to_vec(),
            arrow_data: vec![],
        }
    }

    pub fn encoded_batch(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
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
                    encoded_dictionaries.push(self.dictionary_batch_to_bytes(
                        dict_id,
                        column.as_ref(),
                        write_options,
                        batch.schema().is_little_endian,
                    ));
                }
            }
        }

        let encoded_message = self.record_batch_to_bytes(batch, write_options);

        Ok((encoded_dictionaries, encoded_message))
    }

    /// Write a `RecordBatch` into two sets of bytes, one for the header (gen::Schema::Message) and the
    /// other for the batch's data
    fn record_batch_to_bytes(
        &self,
        batch: &RecordBatch,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<gen::Message::FieldNode> = vec![];
        let mut buffers: Vec<gen::Schema::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];
        let mut offset = 0;
        for array in batch.columns() {
            write(
                array.as_ref(),
                &mut buffers,
                &mut arrow_data,
                &mut nodes,
                &mut offset,
                batch.schema().is_little_endian,
            )
        }

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);

        let root = {
            let mut batch_builder = gen::Message::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(batch.num_rows() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            let b = batch_builder.finish();
            b.as_union_value()
        };
        // create an gen::Schema::Message
        let mut message = gen::Message::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(gen::Message::MessageHeader::RecordBatch);
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

    /// Write dictionary values into two sets of bytes, one for the header (gen::Schema::Message) and the
    /// other for the data
    fn dictionary_batch_to_bytes(
        &self,
        dict_id: i64,
        array: &dyn Array,
        write_options: &IpcWriteOptions,
        is_little_endian: bool,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<gen::Message::FieldNode> = vec![];
        let mut buffers: Vec<gen::Schema::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];

        write_dictionary(
            array,
            &mut buffers,
            &mut arrow_data,
            &mut nodes,
            &mut 0,
            is_little_endian,
            false,
        );

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);

        let root = {
            let mut batch_builder = gen::Message::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(array.len() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            batch_builder.finish()
        };

        let root = {
            let mut batch_builder = gen::Message::DictionaryBatchBuilder::new(&mut fbb);
            batch_builder.add_id(dict_id);
            batch_builder.add_data(root);
            batch_builder.finish().as_union_value()
        };

        let root = {
            let mut message_builder = gen::Message::MessageBuilder::new(&mut fbb);
            message_builder.add_version(write_options.metadata_version);
            message_builder.add_header_type(gen::Message::MessageHeader::DictionaryBatch);
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
            DataType::Dictionary(d, _) => match d.as_ref() {
                DataType::Int8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<i8>>()
                        .unwrap();
                    array.values()
                }
                DataType::Int16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<i16>>()
                        .unwrap();
                    array.values()
                }
                DataType::Int32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<i32>>()
                        .unwrap();
                    array.values()
                }
                DataType::Int64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<i64>>()
                        .unwrap();
                    array.values()
                }
                DataType::UInt8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<u8>>()
                        .unwrap();
                    array.values()
                }
                DataType::UInt16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<u16>>()
                        .unwrap();
                    array.values()
                }
                DataType::UInt32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<u32>>()
                        .unwrap();
                    array.values()
                }
                DataType::UInt64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<u64>>()
                        .unwrap();
                    array.values()
                }
                _ => unreachable!(),
            },
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

/// Stores the encoded data, which is an gen::Schema::Message, and optional Arrow data
pub struct EncodedData {
    /// An encoded gen::Schema::Message
    pub ipc_message: Vec<u8>,
    /// Arrow buffers to be written, should be an empty vec for schema messages
    pub arrow_data: Vec<u8>,
}
/// Write a message's IPC data and buffers, returning metadata and buffer data lengths written
pub fn write_message<W: Write>(
    writer: &mut W,
    encoded: EncodedData,
    write_options: &IpcWriteOptions,
) -> Result<(usize, usize)> {
    let arrow_data_len = encoded.arrow_data.len();
    if arrow_data_len % 8 != 0 {
        return Err(ArrowError::IPC("Arrow data not aligned".to_string()));
    }

    let a = write_options.alignment - 1;
    let buffer = encoded.ipc_message;
    let flatbuf_size = buffer.len();
    let prefix_size = if write_options.write_legacy_ipc_format {
        4
    } else {
        8
    };
    let aligned_size = (flatbuf_size + prefix_size + a) & !a;
    let padding_bytes = aligned_size - flatbuf_size - prefix_size;

    write_continuation(writer, &write_options, (aligned_size - prefix_size) as i32)?;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer.write_all(&buffer)?;
    }
    // write padding
    writer.write_all(&vec![0; padding_bytes])?;

    // write arrow data
    let body_len = if arrow_data_len > 0 {
        write_body_buffers(writer, &encoded.arrow_data)?
    } else {
        0
    };

    Ok((aligned_size, body_len))
}

fn write_body_buffers<W: Write>(mut writer: W, data: &[u8]) -> Result<usize> {
    let len = data.len() as u32;
    let pad_len = pad_to_8(len) as u32;
    let total_len = len + pad_len;

    // write body buffer
    writer.write_all(data)?;
    if pad_len > 0 {
        writer.write_all(&vec![0u8; pad_len as usize][..])?;
    }

    writer.flush()?;
    Ok(total_len as usize)
}

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
pub fn write_continuation<W: Write>(
    writer: &mut W,
    write_options: &IpcWriteOptions,
    total_len: i32,
) -> Result<usize> {
    let mut written = 8;

    // the version of the writer determines whether continuation markers should be added
    match write_options.metadata_version {
        gen::Schema::MetadataVersion::V1
        | gen::Schema::MetadataVersion::V2
        | gen::Schema::MetadataVersion::V3 => {
            unreachable!("Options with the metadata version cannot be created")
        }
        gen::Schema::MetadataVersion::V4 => {
            if !write_options.write_legacy_ipc_format {
                // v0.15.0 format
                writer.write_all(&CONTINUATION_MARKER)?;
                written = 4;
            }
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        gen::Schema::MetadataVersion::V5 => {
            // write continuation marker and message length
            writer.write_all(&CONTINUATION_MARKER)?;
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        z => panic!("Unsupported gen::Schema::MetadataVersion {:?}", z),
    };

    writer.flush()?;

    Ok(written)
}

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
pub(crate) fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}
