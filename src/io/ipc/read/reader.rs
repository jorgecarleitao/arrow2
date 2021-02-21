//! Arrow IPC File and Stream Readers
//!
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::super::{convert, gen};
use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;

type ArrayRef = Arc<dyn Array>;

/// Arrow File reader
pub struct FileReader<R: Read + Seek> {
    /// Buffered file reader that supports reading and seeking
    reader: BufReader<R>,

    /// The schema that is read from the file header
    schema: Schema,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<gen::File::Block>,

    /// A counter to keep track of the current block that should be read
    current_block: usize,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_field: Vec<Option<ArrayRef>>,

    /// Metadata version
    metadata_version: gen::Schema::MetadataVersion,
}

impl<R: Read + Seek> FileReader<R> {
    /// Try to create a new file reader
    ///
    /// Returns errors if the file does not meet the Arrow Format header and footer
    /// requirements
    pub fn try_new(reader: R) -> Result<Self> {
        let mut reader = BufReader::new(reader);
        // check if header and footer contain correct magic bytes
        let mut magic_buffer: [u8; 6] = [0; 6];
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::IPC(
                "Arrow file does not contain correct header".to_string(),
            ));
        }
        reader.seek(SeekFrom::End(-6))?;
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::IPC(
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
            .map_err(|err| ArrowError::IPC(format!("Unable to get root as footer: {:?}", err)))?;

        let blocks = footer.recordBatches().ok_or_else(|| {
            ArrowError::IPC("Unable to get record batches from IPC Footer".to_string())
        })?;

        let total_blocks = blocks.len();

        let ipc_schema = footer.schema().unwrap();
        let schema = convert::fb_to_schema(ipc_schema);

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

            let message = gen::Message::root_as_message(&block_data[..]).map_err(|err| {
                ArrowError::IPC(format!("Unable to get root as message: {:?}", err))
            })?;

            match message.header_type() {
                gen::Message::MessageHeader::DictionaryBatch => {
                    let block_offset = block.offset() as u64 + block.metaDataLength() as u64;
                    let batch = message.header_as_dictionary_batch().unwrap();
                    read_dictionary(
                        batch,
                        &schema,
                        &mut dictionaries_by_field,
                        &mut reader,
                        block_offset,
                    )?;
                }
                t => {
                    return Err(ArrowError::IPC(format!(
                        "Expecting DictionaryBatch in dictionary blocks, found {:?}.",
                        t
                    )));
                }
            };
        }

        Ok(Self {
            reader,
            schema,
            blocks: blocks.to_vec(),
            current_block: 0,
            total_blocks,
            dictionaries_by_field,
            metadata_version: footer.version(),
        })
    }

    /// Return the number of batches in the file
    pub fn num_batches(&self) -> usize {
        self.total_blocks
    }

    /// Return the schema of the file
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Read a specific record batch
    ///
    /// Sets the current block to the index, allowing random reads
    pub fn set_index(&mut self, index: usize) -> Result<()> {
        if index >= self.total_blocks {
            Err(ArrowError::IPC(format!(
                "Cannot set batch to index {} from {} total batches",
                index, self.total_blocks
            )))
        } else {
            self.current_block = index;
            Ok(())
        }
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>> {
        let block = self.blocks[self.current_block];
        self.current_block += 1;

        // read length
        self.reader.seek(SeekFrom::Start(block.offset() as u64))?;
        let mut meta_buf = [0; 4];
        self.reader.read_exact(&mut meta_buf)?;
        if meta_buf == CONTINUATION_MARKER {
            // continuation marker encountered, read message next
            self.reader.read_exact(&mut meta_buf)?;
        }
        let meta_len = i32::from_le_bytes(meta_buf);

        let mut block_data = vec![0; meta_len as usize];
        self.reader.read_exact(&mut block_data)?;

        let message = gen::Message::root_as_message(&block_data[..])
            .map_err(|err| ArrowError::IPC(format!("Unable to get root as footer: {:?}", err)))?;

        // some old test data's footer metadata is not set, so we account for that
        if self.metadata_version != gen::Schema::MetadataVersion::V1
            && message.version() != self.metadata_version
        {
            return Err(ArrowError::IPC(
                "Could not read IPC message as metadata versions mismatch".to_string(),
            ));
        }

        match message.header_type() {
            gen::Message::MessageHeader::Schema => Err(ArrowError::IPC(
                "Not expecting a schema when messages are read".to_string(),
            )),
            gen::Message::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IPC("Unable to read IPC message as record batch".to_string())
                })?;
                read_record_batch(
                    batch,
                    self.schema().clone(),
                    &self.dictionaries_by_field,
                    &mut self.reader,
                    block.offset() as u64 + block.metaDataLength() as u64,
                )
                .map(Some)
            }
            gen::Message::MessageHeader::NONE => Ok(None),
            t => Err(ArrowError::IPC(format!(
                "Reading types other than record batches not yet supported, unable to read {:?}",
                t
            ))),
        }
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        // get current block
        if self.current_block < self.total_blocks {
            self.maybe_next().transpose()
        } else {
            None
        }
    }
}

impl<R: Read + Seek> RecordBatchReader for FileReader<R> {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use crate::io::json_integration::{to_record_batch, ArrowJson};

    use super::*;

    use std::{collections::HashMap, convert::TryFrom, fs::File};

    use flate2::read::GzDecoder;

    fn test_file(version: &str, file_name: &str) {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, file_name
        ))
        .unwrap();

        let reader = FileReader::try_new(file).unwrap();

        // read expected JSON output
        let (schema, batches) = read_gzip_json(version, file_name);

        assert_eq!(&schema, reader.schema());

        batches
            .iter()
            .zip(reader.map(|x| x.unwrap()))
            .for_each(|(lhs, rhs)| {
                assert_eq!(lhs, &rhs);
            });
    }

    #[test]
    fn read_generated_100_primitive() {
        test_file("1.0.0-littleendian", "generated_primitive");
    }

    #[test]
    fn read_generated_100_datetime() {
        test_file("1.0.0-littleendian", "generated_datetime");
    }

    #[test]
    fn read_generated_100_null_trivial() {
        test_file("1.0.0-littleendian", "generated_null_trivial");
    }

    #[test]
    fn read_generated_100_null() {
        test_file("1.0.0-littleendian", "generated_null");
    }

    #[test]
    fn read_generated_100_primitive_zerolength() {
        test_file("1.0.0-littleendian", "generated_primitive_zerolength");
    }

    #[test]
    fn read_generated_100_primitive_primitive_no_batches() {
        test_file("1.0.0-littleendian", "generated_primitive_no_batches");
    }

    #[test]
    fn read_generated_100_dictionary() {
        test_file("1.0.0-littleendian", "generated_dictionary");
    }

    #[test]
    fn read_generated_100_nested() {
        test_file("1.0.0-littleendian", "generated_nested");
    }

    #[test]
    fn read_generated_100_interval() {
        test_file("1.0.0-littleendian", "generated_interval");
    }

    /// Read gzipped JSON file
    fn read_gzip_json(version: &str, file_name: &str) -> (Schema, Vec<RecordBatch>) {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
            testdata, version, file_name
        ))
        .unwrap();
        let mut gz = GzDecoder::new(&file);
        let mut s = String::new();
        gz.read_to_string(&mut s).unwrap();
        // convert to Arrow JSON
        let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();

        let schema = serde_json::to_value(arrow_json.schema).unwrap();
        let schema = Schema::try_from(&schema).unwrap();

        // read dictionaries
        let mut dictionaries = HashMap::new();
        if let Some(dicts) = &arrow_json.dictionaries {
            for json_dict in dicts {
                // TODO: convert to a concrete Arrow type
                dictionaries.insert(json_dict.id, json_dict);
            }
        }

        let batches = arrow_json
            .batches
            .iter()
            .map(|batch| to_record_batch(&schema, batch, &dictionaries))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        (schema, batches)
    }
}
