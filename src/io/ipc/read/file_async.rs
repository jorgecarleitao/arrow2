//! Async reader for Arrow IPC files
use std::io::SeekFrom;
use std::sync::Arc;

use arrow_format::ipc::{
    planus::{ReadAsRoot, Vector},
    BlockRef, FooterRef, MessageHeaderRef, MessageRef,
};
use futures::{
    stream::BoxStream, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream, StreamExt,
};

use crate::array::*;
use crate::chunk::Chunk;
use crate::datatypes::{Field, Schema};
use crate::error::{ArrowError, Result};
use crate::io::ipc::{IpcSchema, ARROW_MAGIC, CONTINUATION_MARKER};

use super::common::{read_dictionary, read_record_batch};
use super::reader::get_serialized_batch;
use super::schema::fb_to_schema;
use super::Dictionaries;
use super::FileMetadata;

/// Async reader for Arrow IPC files
pub struct FileStream<'a> {
    stream: BoxStream<'a, Result<Chunk<Arc<dyn Array>>>>,
    metadata: FileMetadata,
    schema: Schema,
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
        let schema = if let Some(projection) = projection.as_ref() {
            projection.windows(2).for_each(|x| {
                assert!(
                    x[0] < x[1],
                    "IPC projection must be ordered and non-overlapping",
                )
            });
            let fields = projection
                .iter()
                .map(|&x| metadata.schema.fields[x].clone())
                .collect::<Vec<_>>();
            Schema {
                fields,
                metadata: metadata.schema.metadata.clone(),
            }
        } else {
            metadata.schema.clone()
        };

        let stream = Self::stream(reader, metadata.clone(), projection);
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
        &self.schema
    }

    fn stream<R>(
        mut reader: R,
        metadata: FileMetadata,
        projection: Option<Vec<usize>>,
    ) -> BoxStream<'a, Result<Chunk<Arc<dyn Array>>>>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send + 'a,
    {
        async_stream::try_stream! {
            let mut meta_buffer = vec![];
            let mut block_buffer = vec![];
            for block in 0..metadata.blocks.len() {
                let chunk = read_batch(
                    &mut reader,
                    &metadata,
                    projection.as_deref(),
                    block,
                    &mut meta_buffer,
                    &mut block_buffer,
                ).await?;
                yield chunk;
            }
        }
        .boxed()
    }
}

impl<'a> Stream for FileStream<'a> {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

/// Read the metadata from an IPC file.
pub async fn read_file_metadata_async<R>(reader: &mut R) -> Result<FileMetadata>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    // Check header
    let mut magic = [0; 6];
    reader.read_exact(&mut magic).await?;
    if magic != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
            "file does not contain correct Arrow header".to_string(),
        ));
    }
    // Check footer
    reader.seek(SeekFrom::End(-6)).await?;
    reader.read_exact(&mut magic).await?;
    if magic != ARROW_MAGIC {
        return Err(ArrowError::OutOfSpec(
            "file does not contain correct Arrow footer".to_string(),
        ));
    }
    // Get footer size
    let mut footer_size = [0; 4];
    reader.seek(SeekFrom::End(-10)).await?;
    reader.read_exact(&mut footer_size).await?;
    let footer_size = i32::from_le_bytes(footer_size);
    // Read footer
    let mut footer = vec![0; footer_size as usize];
    reader.seek(SeekFrom::End(-10 - footer_size as i64)).await?;
    reader.read_exact(&mut footer).await?;
    let footer = FooterRef::read_as_root(&footer[..])
        .map_err(|err| ArrowError::OutOfSpec(format!("unable to get root as footer: {:?}", err)))?;

    let blocks = footer.record_batches()?.ok_or_else(|| {
        ArrowError::OutOfSpec("unable to get record batches from footer".to_string())
    })?;
    let schema = footer
        .schema()?
        .ok_or_else(|| ArrowError::OutOfSpec("unable to get schema from footer".to_string()))?;
    let (schema, ipc_schema) = fb_to_schema(schema)?;
    let dictionary_blocks = footer.dictionaries()?;
    let dictionaries = if let Some(blocks) = dictionary_blocks {
        read_dictionaries(reader, &schema.fields[..], &ipc_schema, blocks).await?
    } else {
        Default::default()
    };

    Ok(FileMetadata {
        schema,
        ipc_schema,
        blocks: blocks
            .iter()
            .map(|block| Ok(block.try_into()?))
            .collect::<Result<Vec<_>>>()?,
        dictionaries,
    })
}

async fn read_dictionaries<R>(
    reader: &mut R,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    blocks: Vector<'_, BlockRef<'_>>,
) -> Result<Dictionaries>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let mut dictionaries = Default::default();
    let mut data = vec![];
    let mut buffer = vec![];

    for block in blocks {
        let offset = block.offset() as u64;
        read_dictionary_message(reader, offset, &mut data).await?;

        let message = MessageRef::read_as_root(&data).map_err(|err| {
            ArrowError::OutOfSpec(format!("unable to get root as message: {:?}", err))
        })?;
        let header = message
            .header()?
            .ok_or_else(|| ArrowError::oos("message must have a header"))?;
        match header {
            MessageHeaderRef::DictionaryBatch(batch) => {
                buffer.clear();
                buffer.resize(block.body_length() as usize, 0);
                reader.read_exact(&mut buffer).await?;
                let mut cursor = std::io::Cursor::new(&mut buffer);
                read_dictionary(batch, fields, ipc_schema, &mut dictionaries, &mut cursor, 0)?;
            }
            other => {
                return Err(ArrowError::OutOfSpec(format!(
                    "expected DictionaryBatch in dictionary blocks, found {:?}",
                    other,
                )))
            }
        }
    }
    Ok(dictionaries)
}

async fn read_dictionary_message<R>(reader: &mut R, offset: u64, data: &mut Vec<u8>) -> Result<()>
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
    data.clear();
    data.resize(footer_size as usize, 0);
    reader.read_exact(data).await?;

    Ok(())
}

async fn read_batch<R>(
    reader: &mut R,
    metadata: &FileMetadata,
    projection: Option<&[usize]>,
    block: usize,
    meta_buffer: &mut Vec<u8>,
    block_buffer: &mut Vec<u8>,
) -> Result<Chunk<Arc<dyn Array>>>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let block = metadata.blocks[block];
    reader.seek(SeekFrom::Start(block.offset as u64)).await?;
    let mut meta_buf = [0; 4];
    reader.read_exact(&mut meta_buf).await?;
    if meta_buf == CONTINUATION_MARKER {
        reader.read_exact(&mut meta_buf).await?;
    }
    let meta_len = i32::from_le_bytes(meta_buf) as usize;
    meta_buffer.clear();
    meta_buffer.resize(meta_len, 0);
    reader.read_exact(meta_buffer).await?;

    let message = MessageRef::read_as_root(&meta_buffer[..])
        .map_err(|err| ArrowError::oos(format!("unable to parse message: {:?}", err)))?;
    let batch = get_serialized_batch(&message)?;
    block_buffer.clear();
    block_buffer.resize(message.body_length()? as usize, 0);
    reader.read_exact(block_buffer).await?;
    let mut cursor = std::io::Cursor::new(block_buffer);
    let chunk = read_record_batch(
        batch,
        &metadata.schema.fields,
        &metadata.ipc_schema,
        projection,
        &metadata.dictionaries,
        message.version()?,
        &mut cursor,
        0,
    )?;
    Ok(chunk)
}
