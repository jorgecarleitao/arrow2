//! `async` writing of arrow streams
use std::sync::Arc;

use super::super::IpcField;
pub use super::common::WriteOptions;
use super::common::{encode_chunk, DictionaryTracker, EncodedData};
use super::common_async::{write_continuation, write_message};
use super::{default_ipc_fields, schema_to_bytes};

use futures::{future::BoxFuture, AsyncWrite, FutureExt, Sink};
use std::{pin::Pin, task::Poll};

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// An `async` writer to the Apache Arrow stream format.
pub struct StreamWriter<W: AsyncWrite + Unpin + Send> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: WriteOptions,
    /// Whether the stream has been finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,
}

impl<W: AsyncWrite + Unpin + Send> StreamWriter<W> {
    /// Creates a new [`StreamWriter`]
    pub fn new(writer: W, write_options: WriteOptions) -> Self {
        Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
        }
    }

    /// Starts the stream
    pub async fn start(&mut self, schema: &Schema, ipc_fields: Option<&[IpcField]>) -> Result<()> {
        let encoded_message = if let Some(ipc_fields) = ipc_fields {
            EncodedData {
                ipc_message: schema_to_bytes(schema, ipc_fields),
                arrow_data: vec![],
            }
        } else {
            let ipc_fields = default_ipc_fields(&schema.fields);
            EncodedData {
                ipc_message: schema_to_bytes(schema, &ipc_fields),
                arrow_data: vec![],
            }
        };
        write_message(&mut self.writer, encoded_message).await?;
        Ok(())
    }

    /// Writes [`Chunk`] to the stream
    pub async fn write(
        &mut self,
        columns: &Chunk<Arc<dyn Array>>,
        schema: &Schema,
        ipc_fields: Option<&[IpcField]>,
    ) -> Result<()> {
        if self.finished {
            return Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Cannot write to a finished stream".to_string(),
            )));
        }

        let (encoded_dictionaries, encoded_message) = if let Some(ipc_fields) = ipc_fields {
            encode_chunk(
                columns,
                ipc_fields,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        } else {
            let ipc_fields = default_ipc_fields(&schema.fields);
            encode_chunk(
                columns,
                &ipc_fields,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        };

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary).await?;
        }

        write_message(&mut self.writer, encoded_message).await?;
        Ok(())
    }

    /// Finishes the stream
    pub async fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, 0).await?;
        self.finished = true;
        Ok(())
    }

    /// Consumes itself, returning the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// A sink that writes array [`chunks`](Chunk) to an async writer.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use futures::SinkExt;
/// use arrow2::array::{Array, Int32Array};
/// use arrow2::datatypes::{DataType, Field, Schema};
/// use arrow2::chunk::Chunk;
/// # use arrow2::io::ipc::write::stream_async::StreamSink;
/// # futures::executor::block_on(async move {
/// let schema = Schema::from(vec![
///     Field::new("values", DataType::Int32, true),
/// ]);
///
/// let mut buffer = vec![];
/// let mut sink = StreamSink::new(
///     &mut buffer,
///     schema,
///     None,
///     Default::default(),
/// );
///
/// for i in 0..3 {
///     let values = Int32Array::from(&[Some(i), None]);
///     let chunk = Chunk::new(vec![Arc::new(values) as Arc<dyn Array>]);
///     sink.feed(chunk).await?;
/// }
/// sink.close().await?;
/// # arrow2::error::Result::Ok(())
/// # }).unwrap();
/// ```
pub struct StreamSink<'a, W: AsyncWrite + Unpin + Send + 'a> {
    sink: Option<StreamWriter<W>>,
    task: Option<BoxFuture<'a, Result<Option<StreamWriter<W>>>>>,
    schema: Arc<Schema>,
    ipc_fields: Arc<Option<Vec<IpcField>>>,
}

impl<'a, W> StreamSink<'a, W>
where
    W: AsyncWrite + Unpin + Send + 'a,
{
    /// Create a new [`StreamSink`].
    pub fn new(
        writer: W,
        schema: Schema,
        ipc_fields: Option<&[IpcField]>,
        write_options: WriteOptions,
    ) -> Self {
        let mut sink = StreamWriter::new(writer, write_options);
        let schema = Arc::new(schema);
        let s = schema.clone();
        let ipc_fields = Arc::new(ipc_fields.map(|f| f.to_vec()));
        let f = ipc_fields.clone();
        let task = Some(
            async move {
                sink.start(&s, f.as_deref()).await?;
                Ok(Some(sink))
            }
            .boxed(),
        );
        Self {
            sink: None,
            task,
            schema,
            ipc_fields,
        }
    }

    fn poll_complete(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        if let Some(task) = &mut self.task {
            match futures::ready!(task.poll_unpin(cx)) {
                Ok(sink) => {
                    self.sink = sink;
                    self.task = None;
                    Poll::Ready(Ok(()))
                }
                Err(error) => {
                    self.task = None;
                    Poll::Ready(Err(error))
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<'a, W> Sink<Chunk<Arc<dyn Array>>> for StreamSink<'a, W>
where
    W: AsyncWrite + Unpin + Send,
{
    type Error = ArrowError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        self.get_mut().poll_complete(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Chunk<Arc<dyn Array>>) -> Result<()> {
        let this = self.get_mut();
        if let Some(mut sink) = this.sink.take() {
            let schema = this.schema.clone();
            let fields = this.ipc_fields.clone();
            this.task = Some(
                async move {
                    sink.write(&item, &schema, fields.as_deref()).await?;
                    Ok(Some(sink))
                }
                .boxed(),
            );
            Ok(())
        } else {
            Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "writer closed".to_string(),
            )))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        self.get_mut().poll_complete(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        match this.poll_complete(cx) {
            Poll::Ready(Ok(())) => {
                if let Some(mut sink) = this.sink.take() {
                    this.task = Some(
                        async move {
                            sink.finish().await?;
                            Ok(None)
                        }
                        .boxed(),
                    );
                    this.poll_complete(cx)
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            res => res,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{SinkExt, TryStreamExt};

    use crate::{
        array::{Array, Float32Array, Int32Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
        io::ipc::read::stream_async::{read_stream_metadata_async, AsyncStreamReader},
    };

    use super::StreamSink;

    // Verify round trip data integrity when using async read + write.
    #[test]
    fn test_stream_sink_roundtrip() {
        futures::executor::block_on(async move {
            let mut data = vec![];
            for i in 0..5 {
                let a1 = Int32Array::from(&[Some(i), None, Some(i + 1)]);
                let a2 = Float32Array::from(&[None, Some(i as f32), None]);
                let chunk = Chunk::new(vec![
                    Arc::new(a1) as Arc<dyn Array>,
                    Arc::new(a2) as Arc<dyn Array>,
                ]);
                data.push(chunk);
            }
            let schema = Schema::from(vec![
                Field::new("a1", DataType::Int32, true),
                Field::new("a2", DataType::Float32, true),
            ]);

            let mut buffer = vec![];
            let mut sink = StreamSink::new(&mut buffer, schema, None, Default::default());
            for chunk in &data {
                sink.feed(chunk.clone()).await.unwrap();
            }
            sink.close().await.unwrap();
            drop(sink);

            let mut reader = &buffer[..];
            let metadata = read_stream_metadata_async(&mut reader).await.unwrap();
            let stream = AsyncStreamReader::new(reader, metadata);
            let out = stream.try_collect::<Vec<_>>().await.unwrap();
            for i in 0..5 {
                assert_eq!(data[i], out[i]);
            }
        })
    }
}
