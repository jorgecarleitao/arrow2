//! `async` writing of arrow streams
use std::sync::Arc;

use super::super::IpcField;
pub use super::common::WriteOptions;
use super::common::{encode_chunk, DictionaryTracker, EncodedData};
use super::common_async::{write_continuation, write_message};
use super::{default_ipc_fields, schema_to_bytes, Record};

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

/// A sink that writes array [`chunks`](Chunk) as an IPC stream.
///
/// The stream header is automatically written before writing the first chunk.
///
/// The sink uses the same `ipc_fields` projection and `write_options` for each chunk.
/// For more fine-grained control over those parameters, see [`StreamWriter`].
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
///     sink.feed(chunk.into()).await?;
/// }
/// sink.close().await?;
/// # arrow2::error::Result::Ok(())
/// # }).unwrap();
/// ```
pub struct StreamSink<'a, W: AsyncWrite + Unpin + Send + 'a> {
    writer: Option<W>,
    task: Option<BoxFuture<'a, Result<Option<W>>>>,
    options: WriteOptions,
    dictionary_tracker: DictionaryTracker,
    fields: Vec<IpcField>,
}

impl<'a, W> StreamSink<'a, W>
where
    W: AsyncWrite + Unpin + Send + 'a,
{
    /// Create a new [`StreamSink`].
    pub fn new(
        writer: W,
        schema: Schema,
        ipc_fields: Option<Vec<IpcField>>,
        write_options: WriteOptions,
    ) -> Self {
        let fields = ipc_fields.unwrap_or_else(|| default_ipc_fields(&schema.fields));
        let task = Some(Self::start(writer, &schema, &fields[..]));
        Self {
            writer: None,
            task,
            fields,
            dictionary_tracker: DictionaryTracker::new(false),
            options: write_options,
        }
    }

    fn start(
        mut writer: W,
        schema: &Schema,
        ipc_fields: &[IpcField],
    ) -> BoxFuture<'a, Result<Option<W>>> {
        let message = EncodedData {
            ipc_message: schema_to_bytes(schema, ipc_fields),
            arrow_data: vec![],
        };
        async move {
            write_message(&mut writer, message).await?;
            Ok(Some(writer))
        }
        .boxed()
    }

    fn write(&mut self, record: &Record) -> Result<()> {
        let Record { columns, fields } = record;
        let fields = fields.as_ref().unwrap_or(&self.fields);
        let (dictionaries, message) =
            encode_chunk(columns, fields, &mut self.dictionary_tracker, &self.options)?;

        if let Some(mut writer) = self.writer.take() {
            self.task = Some(
                async move {
                    for d in dictionaries {
                        write_message(&mut writer, d).await?;
                    }
                    write_message(&mut writer, message).await?;
                    Ok(Some(writer))
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

    fn poll_complete(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        if let Some(task) = &mut self.task {
            match futures::ready!(task.poll_unpin(cx)) {
                Ok(writer) => {
                    self.writer = writer;
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

impl<'a, W> Sink<Record> for StreamSink<'a, W>
where
    W: AsyncWrite + Unpin + Send,
{
    type Error = ArrowError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        self.get_mut().poll_complete(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Record) -> Result<()> {
        self.get_mut().write(&item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        self.get_mut().poll_complete(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        match this.poll_complete(cx) {
            Poll::Ready(Ok(())) => {
                if let Some(mut writer) = this.writer.take() {
                    this.task = Some(
                        async move {
                            write_continuation(&mut writer, 0).await?;
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
