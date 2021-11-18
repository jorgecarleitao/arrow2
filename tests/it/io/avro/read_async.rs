use avro_rs::Codec;

use arrow2::error::Result;
use arrow2::io::avro::read;

use super::read::write;

async fn _test_metadata(codec: Codec) -> Result<()> {
    let (data, expected) = write(codec).unwrap();

    let file = &mut &data[..];

    let (_, schema, _, _) = read::read_metadata(file)?;

    assert_eq!(&schema, expected.schema().as_ref());

    Ok(())
}

#[tokio::test]
async fn read_metadata() -> Result<()> {
    _test_metadata(Codec::Null).await
}
