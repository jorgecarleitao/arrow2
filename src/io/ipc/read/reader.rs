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

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::super::{convert, gen};
use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;

type ArrayRef = Arc<dyn Array>;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// The schema that is read from the file header
    schema: Arc<Schema>,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<gen::File::Block>,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_field: Vec<Option<ArrayRef>>,

    /// FileMetadata version
    version: gen::Schema::MetadataVersion,

    is_little_endian: bool,
}

impl FileMetadata {
    /// Returns the schema.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

/// Arrow File reader
pub struct FileReader<'a, R: Read + Seek> {
    reader: &'a mut R,
    metadata: FileMetadata,
    current_block: usize,
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

    let footer = gen::File::root_as_footer(&footer_data[..])
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as footer: {:?}", err)))?;

    let blocks = footer.recordBatches().ok_or_else(|| {
        ArrowError::Ipc("Unable to get record batches from IPC Footer".to_string())
    })?;

    let total_blocks = blocks.len();

    let ipc_schema = footer.schema().unwrap();
    let (schema, is_little_endian) = convert::fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);

    // Create an array of optional dictionary value arrays, one per field.
    let mut dictionaries_by_field = vec![None; schema.fields().len()];
    for block in footer.dictionaries().unwrap() {
        // read length from end of offset
        let mut message_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::Start(block.offset() as u64))?;
        reader.read_exact(&mut message_size)?;
        let footer_len = if message_size == CONTINUATION_MARKER {
            reader.read_exact(&mut message_size)?;
            i32::from_le_bytes(message_size)
        } else {
            i32::from_le_bytes(message_size)
        };

        let mut block_data = vec![0; footer_len as usize];

        reader.read_exact(&mut block_data)?;

        let message = gen::Message::root_as_message(&block_data[..])
            .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;

        match message.header_type() {
            gen::Message::MessageHeader::DictionaryBatch => {
                let block_offset = block.offset() as u64 + block.metaDataLength() as u64;
                let batch = message.header_as_dictionary_batch().unwrap();
                read_dictionary(
                    batch,
                    &schema,
                    is_little_endian,
                    &mut dictionaries_by_field,
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
        dictionaries_by_field,
        version: footer.version(),
    })
}

/// Read the IPC file's metadata
pub fn read_batch<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
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

    let message = gen::Message::root_as_message(&block_data[..])
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as footer: {:?}", err)))?;

    // some old test data's footer metadata is not set, so we account for that
    if metadata.version != gen::Schema::MetadataVersion::V1 && message.version() != metadata.version
    {
        return Err(ArrowError::Ipc(
            "Could not read IPC message as metadata versions mismatch".to_string(),
        ));
    }

    match message.header_type() {
        gen::Message::MessageHeader::Schema => Err(ArrowError::Ipc(
            "Not expecting a schema when messages are read".to_string(),
        )),
        gen::Message::MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or_else(|| {
                ArrowError::Ipc("Unable to read IPC message as record batch".to_string())
            })?;
            read_record_batch(
                batch,
                metadata.schema.clone(),
                metadata.is_little_endian,
                &metadata.dictionaries_by_field,
                reader,
                block.offset() as u64 + block.metaDataLength() as u64,
            )
            .map(Some)
        }
        gen::Message::MessageHeader::NONE => Ok(None),
        t => Err(ArrowError::Ipc(format!(
            "Reading types other than record batches not yet supported, unable to read {:?}",
            t
        ))),
    }
}

impl<'a, R: Read + Seek> FileReader<'a, R> {
    /// Creates a new reader
    pub fn new(reader: &'a mut R, metadata: FileMetadata) -> Self {
        Self {
            reader,
            metadata,
            current_block: 0,
        }
    }

    /// Return the schema of the file
    pub fn schema(&self) -> &Arc<Schema> {
        &self.metadata.schema
    }
}

impl<'a, R: Read + Seek> Iterator for FileReader<'a, R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // get current block
        if self.current_block < self.metadata.total_blocks {
            let block = self.current_block;
            self.current_block += 1;
            read_batch(&mut self.reader, &self.metadata, block).transpose()
        } else {
            None
        }
    }
}

impl<'a, R: Read + Seek> RecordBatchReader for FileReader<'a, R> {
    fn schema(&self) -> &Schema {
        &self.metadata.schema
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::error::Result;
    use crate::io::ipc::common::tests::read_gzip_json;

    use super::*;

    fn test_file(version: &str, file_name: &str) -> Result<()> {
        let testdata = crate::util::test_util::arrow_test_data();
        let mut file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, file_name
        ))?;

        let metadata = read_file_metadata(&mut file)?;
        let reader = FileReader::new(&mut file, metadata);

        // read expected JSON output
        let (schema, batches) = read_gzip_json(version, file_name);

        assert_eq!(&schema, reader.schema().as_ref());

        batches.iter().zip(reader).try_for_each(|(lhs, rhs)| {
            assert_eq!(lhs, &rhs?);
            Result::Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn read_generated_100_primitive() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive")?;
        test_file("1.0.0-bigendian", "generated_primitive")
    }

    #[test]
    fn read_generated_100_primitive_large_offsets() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive_large_offsets")?;
        test_file("1.0.0-bigendian", "generated_primitive_large_offsets")
    }

    #[test]
    fn read_generated_100_datetime() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_datetime")?;
        test_file("1.0.0-bigendian", "generated_datetime")
    }

    #[test]
    fn read_generated_100_null_trivial() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_null_trivial")?;
        test_file("1.0.0-bigendian", "generated_null_trivial")
    }

    #[test]
    fn read_generated_100_null() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_null")?;
        test_file("1.0.0-bigendian", "generated_null")
    }

    #[test]
    fn read_generated_100_primitive_zerolength() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive_zerolength")?;
        test_file("1.0.0-bigendian", "generated_primitive_zerolength")
    }

    #[test]
    fn read_generated_100_primitive_primitive_no_batches() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_primitive_no_batches")?;
        test_file("1.0.0-bigendian", "generated_primitive_no_batches")
    }

    #[test]
    fn read_generated_100_dictionary() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_dictionary")?;
        test_file("1.0.0-bigendian", "generated_dictionary")
    }

    #[test]
    fn read_100_custom_metadata() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_custom_metadata")?;
        test_file("1.0.0-bigendian", "generated_custom_metadata")
    }

    #[test]
    fn read_generated_100_nested_large_offsets() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_nested_large_offsets")?;
        test_file("1.0.0-bigendian", "generated_nested_large_offsets")
    }

    #[test]
    fn read_generated_100_nested() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_nested")?;
        test_file("1.0.0-bigendian", "generated_nested")
    }

    #[test]
    fn read_generated_100_dictionary_unsigned() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_dictionary_unsigned")?;
        test_file("1.0.0-bigendian", "generated_dictionary_unsigned")
    }

    #[test]
    fn read_generated_100_decimal() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_decimal")?;
        test_file("1.0.0-bigendian", "generated_decimal")
    }

    #[test]
    fn read_generated_100_interval() -> Result<()> {
        test_file("1.0.0-littleendian", "generated_interval")?;
        test_file("1.0.0-bigendian", "generated_interval")
    }

    #[test]
    fn read_generated_200_compression_lz4() -> Result<()> {
        test_file("2.0.0-compression", "generated_lz4")
    }

    #[test]
    fn read_generated_200_compression_zstd() -> Result<()> {
        test_file("2.0.0-compression", "generated_zstd")
    }
}
