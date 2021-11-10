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
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_format::ipc;
use arrow_format::ipc::flatbuffers::VerifierOptions;

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::super::convert;
use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// The schema that is read from the file header
    schema: Arc<Schema>,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<ipc::File::Block>,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// Dictionaries associated to each dict_id
    dictionaries: HashMap<usize, Arc<dyn Array>>,

    /// FileMetadata version
    version: ipc::Schema::MetadataVersion,

    is_little_endian: bool,
}

impl FileMetadata {
    /// Returns the schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

/// Arrow File reader
pub struct FileReader<R: Read + Seek> {
    reader: R,
    metadata: FileMetadata,
    current_block: usize,
    projection: Option<(Vec<usize>, Arc<Schema>)>,
}

/// Read the IPC file's metadata
pub fn read_file_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetadata> {
    // check if header and footer contain correct magic bytes
    let mut magic_buffer: [u8; 6] = [0; 6];
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(ArrowError::Ipc(
            "Arrow file does not contain correct header".to_string(),
        ));
    }
    reader.seek(SeekFrom::End(-6))?;
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(ArrowError::Ipc(
            "Arrow file does not contain correct footer".to_string(),
        ));
    }
    // read footer length
    let mut footer_size: [u8; 4] = [0; 4];
    reader.seek(SeekFrom::End(-10))?;
    reader.read_exact(&mut footer_size)?;
    let footer_len = i32::from_le_bytes(footer_size);

    // read footer
    let mut footer_data = vec![0; footer_len as usize];
    reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
    reader.read_exact(&mut footer_data)?;

    // set flatbuffer verification options to the same settings as the C++ arrow implementation.
    // Heuristic: tables in a Arrow flatbuffers buffer must take at least 1 bit
    // each in average (ARROW-11559).
    // Especially, the only recursive table (the `Field` table in Schema.fbs)
    // must have a non-empty `type` member.
    let verifier_options = VerifierOptions {
        max_depth: 128,
        max_tables: footer_len as usize * 8,
        ..Default::default()
    };
    let footer = ipc::File::root_as_footer_with_opts(&verifier_options, &footer_data[..])
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as footer: {:?}", err)))?;

    let blocks = footer.recordBatches().ok_or_else(|| {
        ArrowError::Ipc("Unable to get record batches from IPC Footer".to_string())
    })?;

    let total_blocks = blocks.len();

    let ipc_schema = footer.schema().unwrap();
    let (schema, is_little_endian) = convert::fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);

    let mut dictionaries = Default::default();

    for block in footer.dictionaries().unwrap() {
        // read length from end of offset
        let mut message_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::Start(block.offset() as u64))?;
        reader.read_exact(&mut message_size)?;
        if message_size == CONTINUATION_MARKER {
            reader.read_exact(&mut message_size)?;
        };
        let footer_len = i32::from_le_bytes(message_size);

        let mut block_data = vec![0; footer_len as usize];

        reader.read_exact(&mut block_data)?;

        let message = ipc::Message::root_as_message(&block_data[..])
            .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;

        match message.header_type() {
            ipc::Message::MessageHeader::DictionaryBatch => {
                let block_offset = block.offset() as u64 + block.metaDataLength() as u64;
                let batch = message.header_as_dictionary_batch().unwrap();
                read_dictionary(
                    batch,
                    &schema,
                    is_little_endian,
                    &mut dictionaries,
                    reader,
                    block_offset,
                )?;
            }
            t => {
                return Err(ArrowError::Ipc(format!(
                    "Expecting DictionaryBatch in dictionary blocks, found {:?}.",
                    t
                )));
            }
        };
    }
    Ok(FileMetadata {
        schema,
        is_little_endian,
        blocks: blocks.to_vec(),
        total_blocks,
        dictionaries,
        version: footer.version(),
    })
}

/// Read the IPC file's metadata
pub fn read_batch<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    projection: Option<(&[usize], Arc<Schema>)>,
    block: usize,
) -> Result<Option<RecordBatch>> {
    let block = metadata.blocks[block];

    // read length
    reader.seek(SeekFrom::Start(block.offset() as u64))?;
    let mut meta_buf = [0; 4];
    reader.read_exact(&mut meta_buf)?;
    if meta_buf == CONTINUATION_MARKER {
        // continuation marker encountered, read message next
        reader.read_exact(&mut meta_buf)?;
    }
    let meta_len = i32::from_le_bytes(meta_buf);

    let mut block_data = vec![0; meta_len as usize];
    reader.read_exact(&mut block_data)?;

    let message = ipc::Message::root_as_message(&block_data[..])
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as footer: {:?}", err)))?;

    // some old test data's footer metadata is not set, so we account for that
    if metadata.version != ipc::Schema::MetadataVersion::V1 && message.version() != metadata.version
    {
        return Err(ArrowError::Ipc(
            "Could not read IPC message as metadata versions mismatch".to_string(),
        ));
    }

    match message.header_type() {
        ipc::Message::MessageHeader::Schema => Err(ArrowError::Ipc(
            "Not expecting a schema when messages are read".to_string(),
        )),
        ipc::Message::MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or_else(|| {
                ArrowError::Ipc("Unable to read IPC message as record batch".to_string())
            })?;
            read_record_batch(
                batch,
                metadata.schema.clone(),
                projection,
                metadata.is_little_endian,
                &metadata.dictionaries,
                metadata.version,
                reader,
                block.offset() as u64 + block.metaDataLength() as u64,
            )
            .map(Some)
        }
        ipc::Message::MessageHeader::NONE => Ok(None),
        t => Err(ArrowError::Ipc(format!(
            "Reading types other than record batches not yet supported, unable to read {:?}",
            t
        ))),
    }
}

impl<R: Read + Seek> FileReader<R> {
    /// Creates a new [`FileReader`]. Use `projection` to only take certain columns.
    /// # Panic
    /// Panics iff the projection is not in increasing order (e.g. `[1, 0]` nor `[0, 1, 1]` are valid)
    pub fn new(reader: R, metadata: FileMetadata, projection: Option<Vec<usize>>) -> Self {
        if let Some(projection) = projection.as_ref() {
            projection.windows(2).for_each(|x| {
                assert!(
                    x[0] < x[1],
                    "The projection on IPC must be ordered and non-overlapping"
                );
            });
        }
        let projection = projection.map(|projection| {
            let fields = metadata.schema().fields();
            let fields = projection.iter().map(|x| fields[*x].clone()).collect();
            let schema = Arc::new(Schema {
                fields,
                metadata: metadata.schema().metadata().clone(),
            });
            (projection, schema)
        });
        Self {
            reader,
            metadata,
            projection,
            current_block: 0,
        }
    }

    /// Return the schema of the file
    pub fn schema(&self) -> &Arc<Schema> {
        self.projection
            .as_ref()
            .map(|x| &x.1)
            .unwrap_or(&self.metadata.schema)
    }

    /// Consumes this FileReader, returning the underlying reader
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // get current block
        if self.current_block < self.metadata.total_blocks {
            let block = self.current_block;
            self.current_block += 1;
            read_batch(
                &mut self.reader,
                &self.metadata,
                self.projection
                    .as_ref()
                    .map(|x| (x.0.as_ref(), x.1.clone())),
                block,
            )
            .transpose()
        } else {
            None
        }
    }
}

impl<R: Read + Seek> RecordBatchReader for FileReader<R> {
    fn schema(&self) -> &Schema {
        self.schema().as_ref()
    }
}
