//! APIs to read Arrow streams asynchronously
use std::sync::Arc;

use arrow_format::ipc::planus::ReadAsRoot;
use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::Stream;

use crate::array::*;
use crate::chunk::Chunk;
use crate::error::{ArrowError, Result};

use super::super::CONTINUATION_MARKER;
use super::common::{read_dictionary, read_record_batch};
use super::schema::deserialize_stream_metadata;
use super::Dictionaries;
use super::StreamMetadata;

/// The state of an Arrow stream
enum StreamState<R> {
    /// The stream does not contain new chunks (and it has not been closed)
    Waiting((R, StreamMetadata, Dictionaries)),
    /// The stream contain a new chunk
    Some((R, StreamMetadata, Dictionaries, Chunk<Arc<dyn Array>>)),
}

/// Reads the [`StreamMetadata`] of the Arrow stream asynchronously
pub async fn read_stream_metadata_async<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<StreamMetadata> {
    // determine metadata length
    let mut meta_size: [u8; 4] = [0; 4];
    reader.read_exact(&mut meta_size).await?;
    let meta_len = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_size == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_size).await?;
        }
        i32::from_le_bytes(meta_size)
    };

    let mut meta_buffer = vec![0; meta_len as usize];
    reader.read_exact(&mut meta_buffer).await?;

    deserialize_stream_metadata(&meta_buffer)
}

/// Reads the next item, yielding `None` if the stream has been closed,
/// or a [`StreamState`] otherwise.
async fn _read_next<R: AsyncRead + Unpin + Send>(
    mut reader: R,
    metadata: StreamMetadata,
    mut dictionaries: Dictionaries,
    message_buffer: &mut Vec<u8>,
    data_buffer: &mut Vec<u8>,
) -> Result<Option<StreamState<R>>> {
    // determine metadata length
    let mut meta_length: [u8; 4] = [0; 4];

    match reader.read_exact(&mut meta_length).await {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(Some(StreamState::Waiting((reader, metadata, dictionaries))))
            } else {
                Err(ArrowError::from(e))
            };
        }
    }

    let meta_length = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_length == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_length).await?;
        }
        i32::from_le_bytes(meta_length) as usize
    };

    if meta_length == 0 {
        // the stream has ended, mark the reader as finished
        return Ok(None);
    }

    message_buffer.clear();
    message_buffer.resize(meta_length, 0);
    reader.read_exact(message_buffer).await?;

    let message = arrow_format::ipc::MessageRef::read_as_root(message_buffer).map_err(|err| {
        ArrowError::OutOfSpec(format!("Unable to get root as message: {:?}", err))
    })?;
    let header = message.header()?.ok_or_else(|| {
        ArrowError::oos("IPC: unable to fetch the message header. The file or stream is corrupted.")
    })?;

    match header {
        arrow_format::ipc::MessageHeaderRef::Schema(_) => Err(ArrowError::oos("A stream ")),
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => {
            // read the block that makes up the record batch into a buffer
            data_buffer.clear();
            data_buffer.resize(message.body_length()? as usize, 0);
            reader.read_exact(data_buffer).await?;

            read_record_batch(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                None,
                &dictionaries,
                metadata.version,
                &mut std::io::Cursor::new(data_buffer),
                0,
            )
            .map(|x| Some(StreamState::Some((reader, metadata, dictionaries, x))))
        }
        arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
            // read the block that makes up the dictionary batch into a buffer
            let mut buf = vec![0; message.body_length()? as usize];
            reader.read_exact(&mut buf).await?;

            let mut dict_reader = std::io::Cursor::new(buf);

            read_dictionary(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                &mut dictionaries,
                &mut dict_reader,
                0,
            )?;

            // read the next message until we encounter a Chunk<Arc<dyn Array>> message
            Ok(Some(StreamState::Waiting((reader, metadata, dictionaries))))
        }
        t => Err(ArrowError::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}

/// Reads the next item, yielding `None` if the stream is done,
/// and a [`StreamState`] otherwise.
async fn maybe_next<R: AsyncRead + Unpin + Send>(
    reader: R,
    metadata: StreamMetadata,
    dictionaries: Dictionaries,
) -> Result<Option<StreamState<R>>> {
    _read_next(reader, metadata, dictionaries, &mut vec![], &mut vec![]).await
}

/// Arrow Stream reader.
///
/// A [`Stream`] over an Arrow stream that yields a result of [`StreamState`]s.
/// This is the recommended way to read an arrow stream (by iterating over its data).
///
/// For a more thorough walkthrough consult [this example](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow).
pub struct AsyncStreamReader<R: AsyncRead + Unpin + Send + 'static> {
    metadata: StreamMetadata,
    future: Option<BoxFuture<'static, Result<Option<StreamState<R>>>>>,
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncStreamReader<R> {
    /// Creates a new [`AsyncStreamReader`]
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        let future = Some(Box::pin(maybe_next(reader, metadata.clone(), Default::default())) as _);
        Self { metadata, future }
    }

    /// Return the schema of the stream
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }
}

impl<R: AsyncRead + Unpin + Send> Stream for AsyncStreamReader<R> {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        use std::task::Poll;
        let me = Pin::into_inner(self);

        match &mut me.future {
            Some(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(None)) => {
                    me.future = None;
                    Poll::Ready(None)
                }
                Poll::Ready(Ok(Some(StreamState::Some((
                    reader,
                    metadata,
                    dictionaries,
                    batch,
                ))))) => {
                    me.future = Some(Box::pin(maybe_next(reader, metadata, dictionaries)));
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Ok(Some(StreamState::Waiting(_)))) => Poll::Pending,
                Poll::Ready(Err(err)) => {
                    me.future = None;
                    Poll::Ready(Some(Err(err)))
                }
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(None),
        }
    }
}
