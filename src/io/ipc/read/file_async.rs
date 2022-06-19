//! Async reader for Arrow IPC files
use std::collections::HashMap;
use std::io::SeekFrom;

use arrow_format::ipc::{planus::ReadAsRoot, Block, MessageHeaderRef};
use futures::{
    stream::BoxStream, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream, StreamExt,
};

use crate::array::*;
use crate::chunk::Chunk;
use crate::datatypes::{Field, Schema};
use crate::error::{Error, Result};
use crate::io::ipc::{IpcSchema, ARROW_MAGIC, CONTINUATION_MARKER};

use super::common::{apply_projection, prepare_projection, read_dictionary, read_record_batch};
use super::reader::{deserialize_footer, get_serialized_batch};
use super::Dictionaries;
use super::FileMetadata;
use super::OutOfSpecKind;

/// Async reader for Arrow IPC files
pub struct FileStream<'a> {
    stream: BoxStream<'a, Result<Chunk<Box<dyn Array>>>>,
    schema: Option<Schema>,
    metadata: FileMetadata,
}

impl<'a> FileStream<'a> {
    /// Create a new IPC file reader.
    ///
    /// # Examples
    /// See [`FileSink`](crate::io::ipc::write::file_async::FileSink).
    pub fn new<R>(reader: R, metadata: FileMetadata, projection: Option<Vec<usize>>) -> Self
    where
        R: AsyncRead + AsyncSeek + Unpin + Send + 'a,
    {
        let (projection, schema) = if let Some(projection) = projection {
            let (p, h, fields) = prepare_projection(&metadata.schema.fields, projection);
            let schema = Schema {
                fields,
                metadata: metadata.schema.metadata.clone(),
            };
            (Some((p, h)), Some(schema))
        } else {
            (None, None)
        };

        let stream = Self::stream(reader, None, metadata.clone(), projection);
        Self {
            stream,
            metadata,
            schema,
        }
    }

    /// Get the metadata from the IPC file.
    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
    }

    /// Get the projected schema from the IPC file.
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref().unwrap_or(&self.metadata.schema)
    }

    fn stream<R>(
        mut reader: R,
        mut dictionaries: Option<Dictionaries>,
        metadata: FileMetadata,
        projection: Option<(Vec<usize>, HashMap<usize, usize>)>,
    ) -> BoxStream<'a, Result<Chunk<Box<dyn Array>>>>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send + 'a,
    {
        async_stream::try_stream! {
            // read dictionaries
            cached_read_dictionaries(&mut reader, &metadata, &mut dictionaries).await?;

            let mut meta_buffer = vec![];
            let mut block_buffer = vec![];
            for block in 0..metadata.blocks.len() {
                let chunk = read_batch(
                    &mut reader,
                    dictionaries.as_mut().unwrap(),
                    &metadata,
                    projection.as_ref().map(|x| x.0.as_ref()),
                    block,
                    &mut meta_buffer,
                    &mut block_buffer,
                ).await?;

                let chunk = if let Some((_, map)) = &projection {
                    // re-order according to projection
                    apply_projection(chunk, map)
                } else {
                    chunk
                };

                yield chunk;
            }
        }
        .boxed()
    }
}

impl<'a> Stream for FileStream<'a> {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

/// Reads the footer's length and magic number in footer
async fn read_footer_len<R: AsyncRead + AsyncSeek + Unpin>(reader: &mut R) -> Result<usize> {
    // read footer length and magic number in footer
    reader.seek(SeekFrom::End(-10)).await?;
    let mut footer: [u8; 10] = [0; 10];

    reader.read_exact(&mut footer).await?;
    let footer_len = i32::from_le_bytes(footer[..4].try_into().unwrap());

    if footer[4..] != ARROW_MAGIC {
        return Err(Error::from(OutOfSpecKind::InvalidFooter));
    }
    footer_len
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))
}

/// Read the metadata from an IPC file.
pub async fn read_file_metadata_async<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let footer_size = read_footer_len(reader).await?;
    // Read footer
    let mut footer = vec![0; footer_size];
    reader.seek(SeekFrom::End(-10 - footer_size as i64)).await?;
    reader.read_exact(&mut footer).await?;

    deserialize_footer(&footer, u64::MAX)
}

async fn read_batch<R>(
    mut reader: R,
    dictionaries: &mut Dictionaries,
    metadata: &FileMetadata,
    projection: Option<&[usize]>,
    block: usize,
    meta_buffer: &mut Vec<u8>,
    block_buffer: &mut Vec<u8>,
) -> Result<Chunk<Box<dyn Array>>>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let block = metadata.blocks[block];

    let offset: u64 = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    reader.seek(SeekFrom::Start(offset)).await?;
    let mut meta_buf = [0; 4];
    reader.read_exact(&mut meta_buf).await?;
    if meta_buf == CONTINUATION_MARKER {
        reader.read_exact(&mut meta_buf).await?;
    }

    let meta_len = i32::from_le_bytes(meta_buf)
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::UnexpectedNegativeInteger))?;

    meta_buffer.clear();
    meta_buffer.resize(meta_len, 0);
    reader.read_exact(meta_buffer).await?;

    let message = arrow_format::ipc::MessageRef::read_as_root(meta_buffer)
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferMessage(err)))?;

    let batch = get_serialized_batch(&message)?;

    let block_length: usize = message
        .body_length()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferBodyLength(err)))?
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::UnexpectedNegativeInteger))?;

    block_buffer.clear();
    block_buffer.resize(block_length, 0);
    reader.read_exact(block_buffer).await?;
    let mut cursor = std::io::Cursor::new(block_buffer);

    read_record_batch(
        batch,
        &metadata.schema.fields,
        &metadata.ipc_schema,
        projection,
        dictionaries,
        message
            .version()
            .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferVersion(err)))?,
        &mut cursor,
        0,
        metadata.size,
    )
}

async fn read_dictionaries<R>(
    mut reader: R,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    blocks: &[Block],
) -> Result<Dictionaries>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let mut dictionaries = Default::default();
    let mut data = vec![];
    let mut buffer = vec![];

    for block in blocks {
        let offset: u64 = block
            .offset
            .try_into()
            .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

        let length: usize = block
            .body_length
            .try_into()
            .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

        read_dictionary_message(&mut reader, offset, &mut data).await?;

        let message = arrow_format::ipc::MessageRef::read_as_root(&data)
            .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferMessage(err)))?;

        let header = message
            .header()
            .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferHeader(err)))?
            .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageHeader))?;

        match header {
            MessageHeaderRef::DictionaryBatch(batch) => {
                buffer.clear();
                buffer.resize(length, 0);
                reader.read_exact(&mut buffer).await?;
                let mut cursor = std::io::Cursor::new(&mut buffer);
                read_dictionary(
                    batch,
                    fields,
                    ipc_schema,
                    &mut dictionaries,
                    &mut cursor,
                    0,
                    u64::MAX,
                )?;
            }
            _ => return Err(Error::from(OutOfSpecKind::UnexpectedMessageType)),
        }
    }
    Ok(dictionaries)
}

async fn read_dictionary_message<R>(mut reader: R, offset: u64, data: &mut Vec<u8>) -> Result<()>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let mut message_size = [0; 4];
    reader.seek(SeekFrom::Start(offset)).await?;
    reader.read_exact(&mut message_size).await?;
    if message_size == CONTINUATION_MARKER {
        reader.read_exact(&mut message_size).await?;
    }
    let footer_size = i32::from_le_bytes(message_size);

    let footer_size: usize = footer_size
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    data.clear();
    data.resize(footer_size, 0);
    reader.read_exact(data).await?;

    Ok(())
}

async fn cached_read_dictionaries<R: AsyncRead + AsyncSeek + Unpin>(
    reader: &mut R,
    metadata: &FileMetadata,
    dictionaries: &mut Option<Dictionaries>,
) -> Result<()> {
    match (&dictionaries, metadata.dictionaries.as_deref()) {
        (None, Some(blocks)) => {
            let new_dictionaries = read_dictionaries(
                reader,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                blocks,
            )
            .await?;
            *dictionaries = Some(new_dictionaries);
        }
        (None, None) => {
            *dictionaries = Some(Default::default());
        }
        _ => {}
    };
    Ok(())
}
