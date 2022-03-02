use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    error::ArrowError,
    io::parquet::write::{Encoding, FileStreamer, SchemaDescriptor, WriteOptions},
};
use futures::{future::BoxFuture, AsyncWrite, FutureExt, Sink, TryFutureExt};
use parquet2::metadata::KeyValue;
use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll};

/// Sink that writes array [`chunks`](Chunk) as a Parquet file.
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
    schema: SchemaDescriptor,
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
            metadata: HashMap::default(),
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

                    this.task = Some(writer.end(metadata).map_ok(|_| None).boxed());
                    this.poll_complete(cx)
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{future::BoxFuture, io::Cursor, SinkExt};
    use parquet2::{
        compression::Compression,
        write::{Version, WriteOptions},
    };
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        array::{Array, Float32Array, Int32Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Schema},
        error::Result,
        io::parquet::{
            read::{
                infer_schema, read_columns_many_async, read_metadata_async, RowGroupDeserializer,
            },
            write::Encoding,
        },
    };

    use super::FileSink;

    #[test]
    fn test_parquet_async_roundtrip() {
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
            let encoding = vec![Encoding::Plain, Encoding::Plain];
            let options = WriteOptions {
                write_statistics: true,
                compression: Compression::Uncompressed,
                version: Version::V2,
            };

            let mut buffer = Cursor::new(Vec::new());
            let mut sink =
                FileSink::try_new(&mut buffer, schema.clone(), encoding, options).unwrap();
            sink.metadata
                .insert(String::from("key"), Some("value".to_string()));
            for chunk in &data {
                sink.feed(chunk.clone()).await.unwrap();
            }
            sink.close().await.unwrap();
            drop(sink);

            buffer.set_position(0);
            let metadata = read_metadata_async(&mut buffer).await.unwrap();
            let kv = HashMap::<String, Option<String>>::from_iter(
                metadata
                    .key_value_metadata()
                    .to_owned()
                    .unwrap()
                    .into_iter()
                    .map(|kv| (kv.key, kv.value)),
            );
            assert_eq!(kv.get("key").unwrap(), &Some("value".to_string()));
            let read_schema = infer_schema(&metadata).unwrap();
            assert_eq!(read_schema, schema);
            let factory = || Box::pin(futures::future::ready(Ok(buffer.clone()))) as BoxFuture<_>;

            let mut out = vec![];
            for group in &metadata.row_groups {
                let column_chunks =
                    read_columns_many_async(factory, group, schema.fields.clone(), None)
                        .await
                        .unwrap();
                let chunks =
                    RowGroupDeserializer::new(column_chunks, group.num_rows() as usize, None);
                let mut chunks = chunks.collect::<Result<Vec<_>>>().unwrap();
                out.append(&mut chunks);
            }

            for i in 0..5 {
                assert_eq!(data[i], out[i]);
            }
        })
    }
}
