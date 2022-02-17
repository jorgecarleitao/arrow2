//! Sink interface for writing arrow streams.

use super::{stream_async::StreamWriter, WriteOptions};
use crate::{array::Array, chunk::Chunk, datatypes::Schema, error::ArrowError, io::ipc::IpcField};
use futures::{future::BoxFuture, AsyncWrite, FutureExt, Sink};
use std::{pin::Pin, sync::Arc, task::Poll};

/// A sink that writes array [`chunks`](Chunk) to an async writer.
pub struct IpcSink<'a, W: AsyncWrite + Unpin + Send + 'a> {
    sink: Option<StreamWriter<W>>,
    task: Option<BoxFuture<'a, Result<Option<StreamWriter<W>>, ArrowError>>>,
    schema: Arc<Schema>,
    ipc_fields: Arc<Option<Vec<IpcField>>>,
}

impl<'a, W> IpcSink<'a, W>
where
    W: AsyncWrite + Unpin + Send + 'a,
{
    /// Create a new [`IpcSink`].
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

    fn poll_complete(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), ArrowError>> {
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

impl<'a, W> Sink<Chunk<Arc<dyn Array>>> for IpcSink<'a, W>
where
    W: AsyncWrite + Unpin + Send,
{
    type Error = ArrowError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_complete(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Chunk<Arc<dyn Array>>) -> Result<(), Self::Error> {
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

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_complete(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
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
