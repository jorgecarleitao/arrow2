use ahash::AHashMap;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::Schema;
use crate::error::{Error, Result};
use crate::io::ipc::IpcSchema;

use super::super::{ARROW_MAGIC, CONTINUATION_MARKER};
use super::common::*;
use super::schema::fb_to_schema;
use super::Dictionaries;
use super::OutOfSpecKind;
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

    /// The total size of the file in bytes
    pub(crate) size: u64,
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

    let message_length: usize = message_length
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    data.clear();
    data.try_reserve(message_length)?;
    reader
        .by_ref()
        .take(message_length as u64)
        .read_to_end(data)?;

    Ok(())
}

fn read_dictionary_block<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    block: &arrow_format::ipc::Block,
    dictionaries: &mut Dictionaries,
    message_scratch: &mut Vec<u8>,
    dictionary_scratch: &mut Vec<u8>,
) -> Result<()> {
    let offset: u64 = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::UnexpectedNegativeInteger))?;
    let length: u64 = block
        .meta_data_length
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::UnexpectedNegativeInteger))?;
    read_dictionary_message(reader, offset, message_scratch)?;

    let message = arrow_format::ipc::MessageRef::read_as_root(message_scratch.as_ref())
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferMessage(err)))?;

    let header = message
        .header()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferHeader(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageHeader))?;

    match header {
        arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
            let block_offset = offset + length;
            read_dictionary(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                dictionaries,
                reader,
                block_offset,
                metadata.size,
                dictionary_scratch,
            )
        }
        _ => Err(Error::from(OutOfSpecKind::UnexpectedMessageType)),
    }
}

/// Reads all file's dictionaries, if any
/// This function is IO-bounded
pub fn read_file_dictionaries<R: Read + Seek>(
    reader: &mut R,
    metadata: &FileMetadata,
    scratch: &mut Vec<u8>,
) -> Result<Dictionaries> {
    let mut dictionaries = Default::default();

    let blocks = if let Some(blocks) = metadata.dictionaries.as_deref() {
        blocks
    } else {
        return Ok(AHashMap::new());
    };
    // use a temporary smaller scratch for the messages
    let mut message_scratch = Default::default();

    for block in blocks {
        read_dictionary_block(
            reader,
            metadata,
            block,
            &mut dictionaries,
            &mut message_scratch,
            scratch,
        )?;
    }
    Ok(dictionaries)
}

/// Reads the footer's length and magic number in footer
fn read_footer_len<R: Read + Seek>(reader: &mut R) -> Result<(u64, usize)> {
    // read footer length and magic number in footer
    let end = reader.seek(SeekFrom::End(-10))? + 10;

    let mut footer: [u8; 10] = [0; 10];

    reader.read_exact(&mut footer)?;
    let footer_len = i32::from_le_bytes(footer[..4].try_into().unwrap());

    if footer[4..] != ARROW_MAGIC {
        return Err(Error::from(OutOfSpecKind::InvalidFooter));
    }
    let footer_len = footer_len
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    Ok((end, footer_len))
}

pub(super) fn deserialize_footer(footer_data: &[u8], size: u64) -> Result<FileMetadata> {
    let footer = arrow_format::ipc::FooterRef::read_as_root(footer_data)
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferFooter(err)))?;

    let blocks = footer
        .record_batches()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferRecordBatches(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingRecordBatches))?;

    let blocks = blocks
        .iter()
        .map(|block| {
            block
                .try_into()
                .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferRecordBatches(err)))
        })
        .collect::<Result<Vec<_>>>()?;

    let ipc_schema = footer
        .schema()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferSchema(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingSchema))?;
    let (schema, ipc_schema) = fb_to_schema(ipc_schema)?;

    let dictionaries = footer
        .dictionaries()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferDictionaries(err)))?
        .map(|dictionaries| {
            dictionaries
                .into_iter()
                .map(|block| {
                    block.try_into().map_err(|err| {
                        Error::from(OutOfSpecKind::InvalidFlatbufferRecordBatches(err))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    Ok(FileMetadata {
        schema,
        ipc_schema,
        blocks,
        dictionaries,
        size,
    })
}

/// Read the Arrow IPC file's metadata
pub fn read_file_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetadata> {
    // check if header contain the correct magic bytes
    let mut magic_buffer: [u8; 6] = [0; 6];
    let start = reader.seek(SeekFrom::Current(0))?;
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(Error::from(OutOfSpecKind::InvalidHeader));
    }

    let (end, footer_len) = read_footer_len(reader)?;

    // read footer
    reader.seek(SeekFrom::End(-10 - footer_len as i64))?;

    let mut serialized_footer = vec![];
    serialized_footer.try_reserve(footer_len)?;
    reader
        .by_ref()
        .take(footer_len as u64)
        .read_to_end(&mut serialized_footer)?;

    deserialize_footer(&serialized_footer, end - start)
}

pub(super) fn get_serialized_batch<'a>(
    message: &'a arrow_format::ipc::MessageRef,
) -> Result<arrow_format::ipc::RecordBatchRef<'a>> {
    let header = message
        .header()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferHeader(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageHeader))?;
    match header {
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => Ok(batch),
        _ => Err(Error::from(OutOfSpecKind::UnexpectedMessageType)),
    }
}

/// Reads the record batch at position `index` from the reader.
///
/// This function is useful for random access to the file. For example, if
/// you have indexed the file somewhere else, this allows pruning
/// certain parts of the file.
/// # Panics
/// This function panics iff `index >= metadata.blocks.len()`
#[allow(clippy::too_many_arguments)]
pub fn read_batch<R: Read + Seek>(
    reader: &mut R,
    dictionaries: &Dictionaries,
    metadata: &FileMetadata,
    projection: Option<&[usize]>,
    limit: Option<usize>,
    index: usize,
    message_scratch: &mut Vec<u8>,
    data_scratch: &mut Vec<u8>,
) -> Result<Chunk<Box<dyn Array>>> {
    let block = metadata.blocks[index];

    let offset: u64 = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    // read length
    reader.seek(SeekFrom::Start(offset))?;
    let mut meta_buf = [0; 4];
    reader.read_exact(&mut meta_buf)?;
    if meta_buf == CONTINUATION_MARKER {
        // continuation marker encountered, read message next
        reader.read_exact(&mut meta_buf)?;
    }
    let meta_len = i32::from_le_bytes(meta_buf)
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::UnexpectedNegativeInteger))?;

    message_scratch.clear();
    message_scratch.try_reserve(meta_len)?;
    reader
        .by_ref()
        .take(meta_len as u64)
        .read_to_end(message_scratch)?;

    let message = arrow_format::ipc::MessageRef::read_as_root(message_scratch.as_ref())
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferMessage(err)))?;

    let batch = get_serialized_batch(&message)?;

    let offset: u64 = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let length: u64 = block
        .meta_data_length
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    read_record_batch(
        batch,
        &metadata.schema.fields,
        &metadata.ipc_schema,
        projection,
        limit,
        dictionaries,
        message
            .version()
            .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferVersion(err)))?,
        reader,
        offset + length,
        metadata.size,
        data_scratch,
    )
}

/// An iterator of [`Chunk`]s from an Arrow IPC file.
pub struct FileReader<R: Read + Seek> {
    reader: R,
    metadata: FileMetadata,
    // the dictionaries are going to be read
    dictionaries: Option<Dictionaries>,
    current_block: usize,
    projection: Option<(Vec<usize>, AHashMap<usize, usize>, Schema)>,
    remaining: usize,
    data_scratch: Vec<u8>,
    message_scratch: Vec<u8>,
}

impl<R: Read + Seek> FileReader<R> {
    /// Creates a new [`FileReader`]. Use `projection` to only take certain columns.
    /// # Panic
    /// Panics iff the projection is not in increasing order (e.g. `[1, 0]` nor `[0, 1, 1]` are valid)
    pub fn new(
        reader: R,
        metadata: FileMetadata,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
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
            remaining: limit.unwrap_or(usize::MAX),
            current_block: 0,
            data_scratch: Default::default(),
            message_scratch: Default::default(),
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
        if self.dictionaries.is_none() {
            self.dictionaries = Some(read_file_dictionaries(
                &mut self.reader,
                &self.metadata,
                &mut self.data_scratch,
            )?);
        };
        Ok(())
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<Chunk<Box<dyn Array>>>;

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
            Some(self.remaining),
            block,
            &mut self.message_scratch,
            &mut self.data_scratch,
        );
        self.remaining -= chunk.as_ref().map(|x| x.len()).unwrap_or_default();

        let chunk = if let Some((_, map, _)) = &self.projection {
            // re-order according to projection
            chunk.map(|chunk| apply_projection(chunk, map))
        } else {
            chunk
        };
        Some(chunk)
    }
}
