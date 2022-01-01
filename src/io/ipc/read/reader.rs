use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_format::ipc;
use arrow_format::ipc::flatbuffers::VerifierOptions;
use arrow_format::ipc::File::Block;

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::io::ipc::IpcSchema;
use crate::record_batch::RecordBatch;

use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;
use super::schema::fb_to_schema;
use super::Dictionaries;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// The schema that is read from the file footer
    pub schema: Arc<Schema>,

    /// The files' [`IpcSchema`]
    pub ipc_schema: IpcSchema,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<ipc::File::Block>,

    /// Dictionaries associated to each dict_id
    dictionaries: Dictionaries,

    /// FileMetadata version
    version: ipc::Schema::MetadataVersion,
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
    buffer: Vec<u8>,
}

fn read_dictionary_message<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    data: &mut Vec<u8>,
) -> Result<()> {
    let mut message_size: [u8; 4] = [0; 4];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut message_size)?;
    if message_size == CONTINUATION_MARKER {
        reader.read_exact(&mut message_size)?;
    };
    let footer_len = i32::from_le_bytes(message_size);

    // prepare `data` to read the message
    data.clear();
    data.resize(footer_len as usize, 0);

    reader.read_exact(data)?;
    Ok(())
}

fn read_dictionaries<R: Read + Seek>(
    reader: &mut R,
    schema: &Schema,
    ipc_schema: &IpcSchema,
    blocks: &[Block],
) -> Result<Dictionaries> {
    let mut dictionaries = Default::default();
    let mut data = vec![];

    for block in blocks {
        let offset = block.offset() as u64;
        let length = block.metaDataLength() as u64;
        read_dictionary_message(reader, offset, &mut data)?;

        let message = ipc::Message::root_as_message(&data).map_err(|err| {
            ArrowError::OutOfSpec(format!("Unable to get root as message: {:?}", err))
        })?;

        match message.header_type() {
            ipc::Message::MessageHeader::DictionaryBatch => {
                let block_offset = offset + length;
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::OutOfSpec(
                        "The dictionary message does not have a dictionary header. The file is corrupted.".to_string(),
                    )
                })?;
                read_dictionary(
                    batch,
                    schema.fields(),
                    ipc_schema,
                    &mut dictionaries,
                    reader,
                    block_offset,
                )?;
            }
            t => {
                return Err(ArrowError::OutOfSpec(format!(
                    "Expecting DictionaryBatch in dictionary blocks, found {:?}.",
                    t
                )));
            }
        };
    }
    Ok(dictionaries)
}

/// Read the IPC file's metadata
pub fn read_file_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetadata> {
    // check if header and footer contain correct magic bytes
    let mut magic_buffer: [u8; 6] = [0; 6];
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
            "Arrow file does not contain correct header".to_string(),
        ));
    }
    reader.seek(SeekFrom::End(-6))?;
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
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
        .map_err(|err| ArrowError::OutOfSpec(format!("Unable to get root as footer: {:?}", err)))?;

    let blocks = footer.recordBatches().ok_or_else(|| {
        ArrowError::OutOfSpec("Unable to get record batches from footer".to_string())
    })?;

    let ipc_schema = footer
        .schema()
        .ok_or_else(|| ArrowError::OutOfSpec("Unable to get the schema from footer".to_string()))?;
    let (schema, ipc_schema) = fb_to_schema(ipc_schema)?;
    let schema = Arc::new(schema);

    let dictionary_blocks = footer.dictionaries();

    let dictionaries = if let Some(blocks) = dictionary_blocks {
        read_dictionaries(reader, &schema, &ipc_schema, blocks)?
    } else {
        Default::default()
    };

    Ok(FileMetadata {
        schema,
        ipc_schema,
        blocks: blocks.to_vec(),
        dictionaries,
        version: footer.version(),
    })
}

fn get_serialized_batch<'a>(
    message: &'a ipc::Message::Message,
) -> Result<ipc::Message::RecordBatch<'a>> {
    match message.header_type() {
        ipc::Message::MessageHeader::Schema => Err(ArrowError::OutOfSpec(
            "Not expecting a schema when messages are read".to_string(),
        )),
        ipc::Message::MessageHeader::RecordBatch => {
            message.header_as_record_batch().ok_or_else(|| {
                ArrowError::OutOfSpec("Unable to read IPC message as record batch".to_string())
            })
        }
        t => Err(ArrowError::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?}",
            t
        ))),
    }
}

/// Read a batch from the reader.
pub fn read_batch<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    projection: Option<(&[usize], Arc<Schema>)>,
    block: usize,
    block_data: &mut Vec<u8>,
) -> Result<RecordBatch> {
    let block = metadata.blocks[block];

    // read length
    reader.seek(SeekFrom::Start(block.offset() as u64))?;
    let mut meta_buf = [0; 4];
    reader.read_exact(&mut meta_buf)?;
    if meta_buf == CONTINUATION_MARKER {
        // continuation marker encountered, read message next
        reader.read_exact(&mut meta_buf)?;
    }
    let meta_len = i32::from_le_bytes(meta_buf) as usize;

    block_data.clear();
    block_data.resize(meta_len, 0);
    reader.read_exact(block_data)?;

    let message = ipc::Message::root_as_message(&block_data[..])
        .map_err(|err| ArrowError::OutOfSpec(format!("Unable to get root as footer: {:?}", err)))?;

    let batch = get_serialized_batch(&message)?;

    read_record_batch(
        batch,
        metadata.schema.clone(),
        &metadata.ipc_schema,
        projection,
        &metadata.dictionaries,
        metadata.version,
        reader,
        block.offset() as u64 + block.metaDataLength() as u64,
    )
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
            buffer: vec![],
        }
    }

    /// Return the schema of the file
    pub fn schema(&self) -> &Arc<Schema> {
        self.projection
            .as_ref()
            .map(|x| &x.1)
            .unwrap_or(&self.metadata.schema)
    }

    /// Returns the [`FileMetadata`]
    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
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
        if self.current_block < self.metadata.blocks.len() {
            let block = self.current_block;
            self.current_block += 1;
            Some(read_batch(
                &mut self.reader,
                &self.metadata,
                self.projection
                    .as_ref()
                    .map(|x| (x.0.as_ref(), x.1.clone())),
                block,
                &mut self.buffer,
            ))
        } else {
            None
        }
    }
}
