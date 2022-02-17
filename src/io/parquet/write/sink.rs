use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    error::ArrowError,
    io::parquet::write::{Encoding, FileStreamer, SchemaDescriptor, WriteOptions},
};
use futures::{future::BoxFuture, AsyncWrite, FutureExt, Sink, TryFutureExt};
use std::{pin::Pin, sync::Arc, task::Poll};

/// Sink that writes array [`chunks`](Chunk) to an async writer.
pub struct ParquetSink<'a, W: AsyncWrite + Send + Unpin> {
    writer: Option<FileStreamer<W>>,
    task: Option<BoxFuture<'a, Result<Option<FileStreamer<W>>, ArrowError>>>,
    options: WriteOptions,
    encoding: Vec<Encoding>,
    schema: SchemaDescriptor,
}

impl<'a, W> ParquetSink<'a, W>
where
    W: AsyncWrite + Send + Unpin + 'a,
{
    /// Create a new sink that writes arrays to the provided `writer`.
    /// # Error
    /// If the Arrow schema can't be converted to a valid Parquet schema.
    pub fn try_new(
        writer: W,
        schema: Schema,
        encoding: Vec<Encoding>,
        options: WriteOptions,
    ) -> Result<Self, ArrowError> {
        let mut writer = FileStreamer::try_new(writer, schema.clone(), options)?;
        let schema = crate::io::parquet::write::to_parquet_schema(&schema)?;
        let task = Some(
            async move {
                writer.start().await?;
                Ok(Some(writer))
            }
            .boxed(),
        );
        Ok(Self {
            writer: None,
            task,
            options,
            schema,
            encoding,
        })
    }

    fn poll_complete(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ArrowError>> {
        if let Some(task) = &mut self.task {
            match futures::ready!(task.poll_unpin(cx)) {
                Ok(writer) => {
                    self.task = None;
                    self.writer = writer;
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

impl<'a, W> Sink<Chunk<Arc<dyn Array>>> for ParquetSink<'a, W>
where
    W: AsyncWrite + Send + Unpin + 'a,
{
    type Error = ArrowError;

    fn start_send(self: Pin<&mut Self>, item: Chunk<Arc<dyn Array>>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(mut writer) = this.writer.take() {
            let count = item.len();
            let rows = crate::io::parquet::write::row_group_iter(
                item,
                this.encoding.clone(),
                this.schema.columns().to_vec(),
                this.options,
            );
            this.task = Some(Box::pin(async move {
                writer.write(rows, count).await?;
                Ok(Some(writer))
            }));
            Ok(())
        } else {
            Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "writer closed".to_string(),
            )))
        }
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.get_mut().poll_complete(cx)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.get_mut().poll_complete(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match futures::ready!(this.poll_complete(cx)) {
            Ok(()) => {
                let writer = this.writer.take();
                if let Some(writer) = writer {
                    this.task = Some(writer.end(None).map_ok(|_| None).boxed());
                    this.poll_complete(cx)
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}
