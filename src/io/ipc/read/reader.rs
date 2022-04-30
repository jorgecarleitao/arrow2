use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::{Field, Schema};
use crate::error::{ArrowError, Result};
use crate::io::ipc::IpcSchema;

use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;
use super::schema::fb_to_schema;
use super::Dictionaries;
use arrow_format::ipc::planus::ReadAsRoot;

/// Metadata of an Arrow IPC file, written in the footer of the file.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// The schema that is read from the file footer
    pub schema: Schema,

    /// The files' [`IpcSchema`]
    pub ipc_schema: IpcSchema,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    pub(crate) blocks: Vec<arrow_format::ipc::Block>,

    /// Dictionaries associated to each dict_id
    pub(crate) dictionaries: Option<Vec<arrow_format::ipc::Block>>,
}

/// Arrow File reader
pub struct FileReader<R: Read + Seek> {
    reader: R,
    metadata: FileMetadata,
    // the dictionaries are going to be read
    dictionaries: Option<Dictionaries>,
    current_block: usize,
    projection: Option<(Vec<usize>, HashMap<usize, usize>, Schema)>,
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
    let message_length = i32::from_le_bytes(message_size);

    // prepare `data` to read the message
    data.clear();
    data.resize(message_length as usize, 0);

    reader.read_exact(data)?;
    Ok(())
}

pub(crate) fn read_dictionaries<R: Read + Seek>(
    reader: &mut R,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    blocks: &[arrow_format::ipc::Block],
) -> Result<Dictionaries> {
    let mut dictionaries = Default::default();
    let mut data = vec![];

    for block in blocks {
        let offset = block.offset as u64;
        let length = block.meta_data_length as u64;
        read_dictionary_message(reader, offset, &mut data)?;

        let message = arrow_format::ipc::MessageRef::read_as_root(&data).map_err(|err| {
            ArrowError::OutOfSpec(format!("Unable to get root as message: {:?}", err))
        })?;

        let header = message
            .header()?
            .ok_or_else(|| ArrowError::oos("Message must have an header"))?;

        match header {
            arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
                let block_offset = offset + length;
                read_dictionary(
                    batch,
                    fields,
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

/// Reads the footer's length and magic number in footer
fn read_footer_len<R: Read + Seek>(reader: &mut R) -> Result<usize> {
    // read footer length and magic number in footer
    reader.seek(SeekFrom::End(-10))?;
    let mut footer: [u8; 10] = [0; 10];

    reader.read_exact(&mut footer)?;
    let footer_len = i32::from_le_bytes(footer[..4].try_into().unwrap());

    if footer[4..] != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
            "Arrow file does not contain correct footer".to_string(),
        ));
    }
    footer_len
        .try_into()
        .map_err(|_| ArrowError::oos("The footer's lenght must be a positive number"))
}

pub(super) fn deserialize_footer(footer_data: &[u8]) -> Result<FileMetadata> {
    let footer = arrow_format::ipc::FooterRef::read_as_root(footer_data)
        .map_err(|err| ArrowError::OutOfSpec(format!("Unable to get root as footer: {:?}", err)))?;

    let blocks = footer.record_batches()?.ok_or_else(|| {
        ArrowError::OutOfSpec("Unable to get record batches from footer".to_string())
    })?;

    let blocks = blocks
        .iter()
        .map(|block| Ok(block.try_into()?))
        .collect::<Result<Vec<_>>>()?;

    let ipc_schema = footer
        .schema()?
        .ok_or_else(|| ArrowError::OutOfSpec("Unable to get the schema from footer".to_string()))?;
    let (schema, ipc_schema) = fb_to_schema(ipc_schema)?;

    let dictionaries = footer
        .dictionaries()?
        .map(|dictionaries| {
            dictionaries
                .into_iter()
                .map(|x| Ok(x.try_into()?))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    Ok(FileMetadata {
        schema,
        ipc_schema,
        blocks,
        dictionaries,
    })
}

/// Read the IPC file's metadata
pub fn read_file_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetadata> {
    // check if header contain the correct magic bytes
    let mut magic_buffer: [u8; 6] = [0; 6];
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
            "Arrow file does not contain correct header".to_string(),
        ));
    }

    let footer_len = read_footer_len(reader)?;

    // read footer
    let mut footer_data = vec![0; footer_len as usize];
    reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
    reader.read_exact(&mut footer_data)?;

    deserialize_footer(&footer_data)

    /*
    // read dictionaries
    metadata.dictionaries = if let Some(blocks) = dictionary_blocks {
        read_dictionaries(
            reader,
            &metadata.schema.fields,
            &metadata.ipc_schema,
            blocks,
        )?
    } else {
        Default::default()
    };*/
}

pub(super) fn get_serialized_batch<'a>(
    message: &'a arrow_format::ipc::MessageRef,
) -> Result<arrow_format::ipc::RecordBatchRef<'a>> {
    let header = message.header()?.ok_or_else(|| {
        ArrowError::oos("IPC: unable to fetch the message header. The file or stream is corrupted.")
    })?;
    match header {
        arrow_format::ipc::MessageHeaderRef::Schema(_) => Err(ArrowError::OutOfSpec(
            "Not expecting a schema when messages are read".to_string(),
        )),
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => Ok(batch),
        t => Err(ArrowError::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?}",
            t
        ))),
    }
}

/// Read a batch from the reader.
pub fn read_batch<R: Read + Seek>(
    reader: &mut R,
    dictionaries: &Dictionaries,
    metadata: &FileMetadata,
    projection: Option<&[usize]>,
    block: usize,
    block_data: &mut Vec<u8>,
) -> Result<Chunk<Arc<dyn Array>>> {
    let block = metadata.blocks[block];

    // read length
    reader.seek(SeekFrom::Start(block.offset as u64))?;
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

    let message = arrow_format::ipc::MessageRef::read_as_root(&block_data[..])
        .map_err(|err| ArrowError::oos(format!("Unable parse message: {:?}", err)))?;

    let batch = get_serialized_batch(&message)?;

    read_record_batch(
        batch,
        &metadata.schema.fields,
        &metadata.ipc_schema,
        projection,
        dictionaries,
        message.version()?,
        reader,
        block.offset as u64 + block.meta_data_length as u64,
    )
}

impl<R: Read + Seek> FileReader<R> {
    /// Creates a new [`FileReader`]. Use `projection` to only take certain columns.
    /// # Panic
    /// Panics iff the projection is not in increasing order (e.g. `[1, 0]` nor `[0, 1, 1]` are valid)
    pub fn new(reader: R, metadata: FileMetadata, projection: Option<Vec<usize>>) -> Self {
        let projection = projection.map(|projection| {
            let (p, h, fields) = prepare_projection(&metadata.schema.fields, projection);
            let schema = Schema {
                fields,
                metadata: metadata.schema.metadata.clone(),
            };
            (p, h, schema)
        });
        Self {
            reader,
            metadata,
            dictionaries: Default::default(),
            projection,
            current_block: 0,
            buffer: vec![],
        }
    }

    /// Return the schema of the file
    pub fn schema(&self) -> &Schema {
        self.projection
            .as_ref()
            .map(|x| &x.2)
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

    fn read_dictionaries(&mut self) -> Result<()> {
        match (
            &mut self.dictionaries,
            self.metadata.dictionaries.as_deref(),
        ) {
            (None, Some(blocks)) => {
                let dictionaries = read_dictionaries(
                    &mut self.reader,
                    &self.metadata.schema.fields,
                    &self.metadata.ipc_schema,
                    blocks,
                )?;
                self.dictionaries = Some(dictionaries);
            }
            (None, None) => {
                self.dictionaries = Some(Default::default());
            }
            _ => {}
        };
        Ok(())
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        // get current block
        if self.current_block == self.metadata.blocks.len() {
            return None;
        }

        match self.read_dictionaries() {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        };

        let block = self.current_block;
        self.current_block += 1;

        let chunk = read_batch(
            &mut self.reader,
            self.dictionaries.as_ref().unwrap(),
            &self.metadata,
            self.projection.as_ref().map(|x| x.0.as_ref()),
            block,
            &mut self.buffer,
        );

        let chunk = if let Some((projection, map, _)) = &self.projection {
            // re-order according to projection
            chunk.map(|chunk| apply_projection(chunk, projection, map))
        } else {
            chunk
        };
        Some(chunk)
    }
}
