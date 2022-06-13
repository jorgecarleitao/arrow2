//! APIs to read Arrow streams asynchronously

use arrow_format::ipc::planus::ReadAsRoot;
use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::Stream;

use crate::array::*;
use crate::chunk::Chunk;
use crate::error::{Error, Result};

use super::super::CONTINUATION_MARKER;
use super::common::{read_dictionary, read_record_batch};
use super::schema::deserialize_stream_metadata;
use super::Dictionaries;
use super::StreamMetadata;

/// A (private) state of stream messages
struct ReadState<R> {
    pub reader: R,
    pub metadata: StreamMetadata,
    pub dictionaries: Dictionaries,
    /// The internal buffer to read data inside the messages (records and dictionaries) to
    pub data_buffer: Vec<u8>,
    /// The internal buffer to read messages to
    pub message_buffer: Vec<u8>,
}

/// The state of an Arrow stream
enum StreamState<R> {
    /// The stream does not contain new chunks (and it has not been closed)
    Waiting(ReadState<R>),
    /// The stream contain a new chunk
    Some((ReadState<R>, Chunk<Box<dyn Array>>)),
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
async fn maybe_next<R: AsyncRead + Unpin + Send>(
    mut state: ReadState<R>,
) -> Result<Option<StreamState<R>>> {
    // determine metadata length
    let mut meta_length: [u8; 4] = [0; 4];

    match state.reader.read_exact(&mut meta_length).await {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(Some(StreamState::Waiting(state)))
            } else {
                Err(Error::from(e))
            };
        }
    }

    let meta_length = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_length == CONTINUATION_MARKER {
            state.reader.read_exact(&mut meta_length).await?;
        }
        i32::from_le_bytes(meta_length) as usize
    };

    if meta_length == 0 {
        // the stream has ended, mark the reader as finished
        return Ok(None);
    }

    state.message_buffer.clear();
    state.message_buffer.resize(meta_length, 0);
    state.reader.read_exact(&mut state.message_buffer).await?;

    let message = arrow_format::ipc::MessageRef::read_as_root(&state.message_buffer)
        .map_err(|err| Error::OutOfSpec(format!("Unable to get root as message: {:?}", err)))?;
    let header = message.header()?.ok_or_else(|| {
        Error::oos("IPC: unable to fetch the message header. The file or stream is corrupted.")
    })?;

    match header {
        arrow_format::ipc::MessageHeaderRef::Schema(_) => {
            Err(Error::oos("A stream cannot contain a schema message"))
        }
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => {
            // read the block that makes up the record batch into a buffer
            let length: usize = message
                .body_length()?
                .try_into()
                .map_err(|_| Error::oos("The body length of a header must be larger than zero"))?;
            state.data_buffer.clear();
            state.data_buffer.resize(length, 0);
            state.reader.read_exact(&mut state.data_buffer).await?;

            read_record_batch(
                batch,
                &state.metadata.schema.fields,
                &state.metadata.ipc_schema,
                None,
                &state.dictionaries,
                state.metadata.version,
                &mut std::io::Cursor::new(&state.data_buffer),
                0,
                state.data_buffer.len() as u64,
            )
            .map(|chunk| Some(StreamState::Some((state, chunk))))
        }
        arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
            // read the block that makes up the dictionary batch into a buffer
            let length: usize = message
                .body_length()?
                .try_into()
                .map_err(|_| Error::oos("The body length of a header must be larger than zero"))?;
            let mut body = vec![0; length];
            state.reader.read_exact(&mut body).await?;

            let file_size = body.len() as u64;

            let mut dict_reader = std::io::Cursor::new(body);

            read_dictionary(
                batch,
                &state.metadata.schema.fields,
                &state.metadata.ipc_schema,
                &mut state.dictionaries,
                &mut dict_reader,
                0,
                file_size,
            )?;

            // read the next message until we encounter a Chunk<Box<dyn Array>> message
            Ok(Some(StreamState::Waiting(state)))
        }
        t => Err(Error::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}

/// A [`Stream`] over an Arrow IPC stream that asynchronously yields [`Chunk`]s.
pub struct AsyncStreamReader<'a, R: AsyncRead + Unpin + Send + 'a> {
    metadata: StreamMetadata,
    future: Option<BoxFuture<'a, Result<Option<StreamState<R>>>>>,
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> AsyncStreamReader<'a, R> {
    /// Creates a new [`AsyncStreamReader`]
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        let state = ReadState {
            reader,
            metadata: metadata.clone(),
            dictionaries: Default::default(),
            data_buffer: Default::default(),
            message_buffer: Default::default(),
        };
        let future = Some(Box::pin(maybe_next(state)) as _);
        Self { metadata, future }
    }

    /// Return the schema of the stream
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }
}

impl<'a, R: AsyncRead + Unpin + Send> Stream for AsyncStreamReader<'a, R> {
    type Item = Result<Chunk<Box<dyn Array>>>;

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
                Poll::Ready(Ok(Some(StreamState::Some((state, batch))))) => {
                    me.future = Some(Box::pin(maybe_next(state)));
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
