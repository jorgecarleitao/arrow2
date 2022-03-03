use std::sync::Arc;

use futures::{SinkExt, StreamExt, TryStreamExt};
use tokio::fs::File;
use tokio_util::compat::*;

use arrow2::array::{Array, Float32Array, Int32Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::ipc::read::stream_async::*;
use arrow2::io::ipc::write::stream_async::StreamSink;

use crate::io::ipc::common::read_gzip_json;

async fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.stream",
        testdata, version, file_name
    ))
    .await?
    .compat();

    let metadata = read_stream_metadata_async(&mut file).await?;
    let mut reader = AsyncStreamReader::new(file, metadata);

    // read expected JSON output
    let (schema, ipc_fields, batches) = read_gzip_json(version, file_name)?;

    assert_eq!(&schema, &reader.metadata().schema);
    assert_eq!(&ipc_fields, &reader.metadata().ipc_schema.fields);

    let mut items = vec![];
    while let Some(item) = reader.next().await {
        items.push(item?)
    }

    batches
        .iter()
        .zip(items.into_iter())
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, &rhs);
        });
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}

#[tokio::test]
async fn test_stream_async_roundtrip() {
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
    let mut sink = StreamSink::new(&mut buffer, schema.clone(), None, Default::default());
    for chunk in &data {
        sink.feed(chunk.clone().into()).await.unwrap();
    }
    sink.close().await.unwrap();
    drop(sink);

    let mut reader = &buffer[..];
    let metadata = read_stream_metadata_async(&mut reader).await.unwrap();
    assert_eq!(schema, metadata.schema);
    let stream = AsyncStreamReader::new(reader, metadata);
    let out = stream.try_collect::<Vec<_>>().await.unwrap();
    for i in 0..5 {
        assert_eq!(data[i], out[i]);
    }
}
