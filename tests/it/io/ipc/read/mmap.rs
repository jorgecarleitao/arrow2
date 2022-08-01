use arrow2::error::Result;
use arrow2::mmap::map_chunk_unchecked;

use super::super::common::read_gzip_json;

#[derive(Clone)]
struct Mmap(pub std::sync::Arc<Vec<u8>>);

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();

    let arrow_file = format!(
        "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
        testdata, version, file_name
    );

    let data = std::fs::read(arrow_file).unwrap();

    let data = Mmap(std::sync::Arc::new(data));

    // read expected JSON output
    let (_schema, _, batches) = read_gzip_json(version, file_name)?;

    let array = unsafe { map_chunk_unchecked(data, 0)? };

    assert_eq!(batches[0].arrays()[0], array);
    Ok(())
}

#[test]
fn read_generated_100_primitive() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive")
}
