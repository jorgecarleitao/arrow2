use std::io::Cursor;

use arrow2::error::Result;
use arrow2::io::ipc::read::read_stream_metadata;
use arrow2::io::ipc::read::StreamReader;
use arrow2::io::ipc::write::stream_async::{StreamWriter, WriteOptions};
use futures::io::Cursor as AsyncCursor;

use crate::io::ipc::common::read_arrow_stream;
use crate::io::ipc::common::read_gzip_json;

async fn test_file(version: &str, file_name: &str) -> Result<()> {
    let (schema, batches) = read_arrow_stream(version, file_name);

    let mut result = AsyncCursor::new(Vec::<u8>::new());

    // write IPC version 5
    {
        let options = WriteOptions { compression: None };
        let mut writer = StreamWriter::new(&mut result, options);
        writer.start(&schema).await?;
        for batch in batches {
            writer.write(&batch).await?;
        }
        writer.finish().await?;
    }
    let result = result.into_inner();

    let mut reader = Cursor::new(result);
    let metadata = read_stream_metadata(&mut reader)?;
    let reader = StreamReader::new(reader, metadata);

    let schema = reader.schema().clone();

    // read expected JSON output
    let (expected_schema, expected_batches) = read_gzip_json(version, file_name).unwrap();

    assert_eq!(schema.as_ref(), &expected_schema);

    let batches = reader
        .map(|x| x.map(|x| x.unwrap()))
        .collect::<Result<Vec<_>>>()?;

    assert_eq!(batches, expected_batches);
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}
