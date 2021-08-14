use std::fs::File;

use arrow2::error::Result;
use arrow2::io::ipc::read::*;

use crate::io::ipc::common::read_gzip_json;

fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.stream",
        testdata, version, file_name
    ))?;

    let metadata = read_stream_metadata(&mut file)?;
    let reader = StreamReader::new(file, metadata);

    // read expected JSON output
    let (schema, batches) = read_gzip_json(version, file_name);

    assert_eq!(&schema, reader.schema().as_ref());

    batches
        .iter()
        .zip(reader.map(|x| x.unwrap()))
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, &rhs);
        });
    Ok(())
}

#[test]
fn read_generated_100_primitive() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive")
}

#[test]
fn read_generated_100_datetime() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_datetime")
}

#[test]
fn read_generated_100_null_trivial() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null_trivial")
}

#[test]
fn read_generated_100_null() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null")
}

#[test]
fn read_generated_100_primitive_zerolength() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive_zerolength")
}

#[test]
fn read_generated_100_primitive_primitive_no_batches() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive_no_batches")
}

#[test]
fn read_generated_100_dictionary() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary")
}

#[test]
fn read_generated_100_nested() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_nested")
}

#[test]
fn read_generated_100_interval() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_interval")
}

#[test]
fn read_generated_100_decimal() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_decimal")
}

#[test]
fn read_generated_100_union() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_union")?;
    test_file("1.0.0-bigendian", "generated_union")
}

#[test]
fn read_generated_017_union() -> Result<()> {
    test_file("0.17.1", "generated_union")
}

#[test]
fn read_generated_200_compression_lz4() -> Result<()> {
    test_file("2.0.0-compression", "generated_lz4")
}

#[test]
fn read_generated_200_compression_zstd() -> Result<()> {
    test_file("2.0.0-compression", "generated_zstd")
}
