use arrow2::array::{Array, Int64Array};
use arrow2::datatypes::DataType;
use arrow2::error::Result;
use arrow2::io::parquet::read::{
    decompress, get_page_stream, page_stream_to_array, read_metadata_async,
};
use futures::{future::BoxFuture, StreamExt};
use s3::Bucket;

mod stream;
use stream::{RangedStreamer, SeekOutput};

#[tokio::main]
async fn main() -> Result<()> {
    let bucket_name = "dev-jorgecardleitao";
    let region = "eu-central-1".parse().unwrap();
    let bucket = Bucket::new_public(bucket_name, region).unwrap();
    let path = "benches_65536.parquet".to_string();

    let (data, _) = bucket.head_object(&path).await.unwrap();
    let length = data.content_length.unwrap() as usize;
    println!("total size in bytes: {}", length);

    let range_get = Box::new(move |start: u64, length: usize| {
        let bucket = bucket.clone();
        let path = path.clone();
        Box::pin(async move {
            let bucket = bucket.clone();
            let path = path.clone();
            // to get a sense of what is being queried in s3
            println!("getting {} bytes starting at {}", length, start);
            let (mut data, _) = bucket
                // -1 because ranges are inclusive in `get_object_range`
                .get_object_range(&path, start, Some(start + length as u64 - 1))
                .await
                .map_err(|x| std::io::Error::new(std::io::ErrorKind::Other, x.to_string()))?;
            println!("got {}/{} bytes starting at {}", data.len(), length, start);
            data.truncate(length);
            Ok(SeekOutput { start, data })
        }) as BoxFuture<'static, std::io::Result<SeekOutput>>
    });

    // at least 4kb per s3 request. Adjust as you like.
    let mut reader = RangedStreamer::new(length, 4 * 1024, range_get);

    let metadata = read_metadata_async(&mut reader).await?;

    // metadata
    println!("{}", metadata.num_rows);

    // pages of the first row group and first column
    // This is IO bounded and SHOULD be done in a shared thread pool (e.g. Tokio)
    let column_metadata = &metadata.row_groups[0].columns()[0];
    let pages = get_page_stream(column_metadata, &mut reader, None, vec![]).await?;

    // decompress the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
    let pages = pages.map(|compressed_page| decompress(compressed_page?, &mut vec![]));

    // deserialize the pages. This is CPU bounded and SHOULD be done in a dedicated thread pool (e.g. Rayon)
    let array =
        page_stream_to_array(pages, &metadata.row_groups[0].columns()[0], DataType::Int64).await?;

    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
    // ... and have fun with it.
    println!("len: {}", array.len());
    println!("null_count: {}", array.null_count());
    Ok(())
}
