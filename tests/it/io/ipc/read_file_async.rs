use arrow2::{
    array::{Array, Float32Array, Int32Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    io::ipc::read::file_async::{read_file_metadata_async, FileStream},
    io::ipc::write::file_async::FileSink,
};
use futures::{io::Cursor, SinkExt, TryStreamExt};
use std::sync::Arc;

#[tokio::test]
async fn test_file_async_roundtrip() {
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

    let mut buffer = Cursor::new(Vec::new());
    let mut sink = FileSink::new(&mut buffer, schema.clone(), None, Default::default());
    for chunk in &data {
        sink.feed(chunk.clone().into()).await.unwrap();
    }
    sink.close().await.unwrap();
    drop(sink);

    buffer.set_position(0);
    let metadata = read_file_metadata_async(&mut buffer).await.unwrap();
    assert_eq!(schema, metadata.schema);
    let stream = FileStream::new(buffer, metadata, None);
    let out = stream.try_collect::<Vec<_>>().await.unwrap();
    for i in 0..5 {
        assert_eq!(data[i], out[i]);
    }
}
