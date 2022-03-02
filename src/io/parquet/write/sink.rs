use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    error::ArrowError,
    io::parquet::write::{Encoding, SchemaDescriptor, WriteOptions},
};
use futures::{future::BoxFuture, AsyncWrite, FutureExt, Sink, TryFutureExt};
use parquet2::metadata::KeyValue;
use parquet2::write::FileStreamer;
use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll};

use super::file::add_arrow_schema;

/// Sink that writes array [`chunks`](Chunk) as a Parquet file.
///
/// Any values in the sink's `metadata` field will be written to the file's footer
/// when the sink is closed.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use futures::SinkExt;
/// use arrow2::array::{Array, Int32Array};
/// use arrow2::datatypes::{DataType, Field, Schema};
/// use arrow2::chunk::Chunk;
/// use arrow2::io::parquet::write::{Encoding, WriteOptions, Compression, Version};
/// # use arrow2::io::parquet::write::FileSink;
/// # futures::executor::block_on(async move {
///
/// let schema = Schema::from(vec![
///     Field::new("values", DataType::Int32, true),
/// ]);
/// let encoding = vec![Encoding::Plain];
/// let options = WriteOptions {
///     write_statistics: true,
///     compression: Compression::Uncompressed,
///     version: Version::V2,
/// };
///
/// let mut buffer = vec![];
/// let mut sink = FileSink::try_new(
///     &mut buffer,
///     schema,
///     encoding,
///     options,
/// )?;
///
/// for i in 0..3 {
///     let values = Int32Array::from(&[Some(i), None]);
///     let chunk = Chunk::new(vec![Arc::new(values) as Arc<dyn Array>]);
///     sink.feed(chunk).await?;
/// }
/// sink.metadata.insert(String::from("key"), Some(String::from("value")));
/// sink.close().await?;
/// # arrow2::error::Result::Ok(())
/// # }).unwrap();
/// ```
pub struct FileSink<'a, W: AsyncWrite + Send + Unpin> {
    writer: Option<FileStreamer<W>>,
    task: Option<BoxFuture<'a, Result<Option<FileStreamer<W>>, ArrowError>>>,
    options: WriteOptions,
    encoding: Vec<Encoding>,
    schema: Schema,
    parquet_schema: SchemaDescriptor,
    /// Key-value metadata that will be written to the file on close.
    pub metadata: HashMap<String, Option<String>>,
}

impl<'a, W> FileSink<'a, W>
where
    W: AsyncWrite + Send + Unpin + 'a,
{
    /// Create a new sink that writes arrays to the provided `writer`.
    ///
    /// # Error
    /// If the Arrow schema can't be converted to a valid Parquet schema.
    pub fn try_new(
        writer: W,
        schema: Schema,
        encoding: Vec<Encoding>,
        options: WriteOptions,
    ) -> Result<Self, ArrowError> {
        // let mut writer = FileStreamer::try_new(writer, schema.clone(), options)?;
        let parquet_schema = crate::io::parquet::write::to_parquet_schema(&schema)?;
        let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());
        let mut writer = FileStreamer::new(writer, parquet_schema.clone(), options, created_by);
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
            parquet_schema,
            metadata: HashMap::default(),
        })
    }

    /// The Arrow [`Schema`] for the file.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The Parquet [`SchemaDescriptor`] for the file.
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        &self.parquet_schema
    }

    /// The write options for the file.
    pub fn options(&self) -> &WriteOptions {
        &self.options
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

impl<'a, W> Sink<Chunk<Arc<dyn Array>>> for FileSink<'a, W>
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
                this.parquet_schema.columns().to_vec(),
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
                    let meta = std::mem::take(&mut this.metadata);
                    let metadata = if meta.is_empty() {
                        None
                    } else {
                        Some(
                            meta.into_iter()
                                .map(|(k, v)| KeyValue::new(k, v))
                                .collect::<Vec<_>>(),
                        )
                    };
                    let kv_meta = add_arrow_schema(&this.schema, metadata);

                    this.task = Some(
                        writer
                            .end(kv_meta)
                            .map_ok(|_| None)
                            .map_err(ArrowError::from)
                            .boxed(),
                    );
                    this.poll_complete(cx)
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}
