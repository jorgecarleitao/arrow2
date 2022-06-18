use std::fs::File;

use arrow2::error::Result;
use arrow2::io::ipc::read::*;

use super::super::common::read_gzip_json;

fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
        testdata, version, file_name
    ))?;

    // read expected JSON output
    let (schema, _, batches) = read_gzip_json(version, file_name)?;

    let metadata = read_file_metadata(&mut file)?;
    let reader = FileReader::new(file, metadata, None);

    assert_eq!(&schema, reader.schema());

    batches.iter().zip(reader).try_for_each(|(lhs, rhs)| {
        assert_eq!(lhs, &rhs?);
        Result::Ok(())
    })?;
    Ok(())
}

#[test]
fn read_generated_100_primitive() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive")?;
    test_file("1.0.0-bigendian", "generated_primitive")
}

#[test]
fn read_generated_100_primitive_large_offsets() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive_large_offsets")?;
    test_file("1.0.0-bigendian", "generated_primitive_large_offsets")
}

#[test]
fn read_generated_100_datetime() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_datetime")?;
    test_file("1.0.0-bigendian", "generated_datetime")
}

#[test]
fn read_generated_100_null_trivial() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null_trivial")?;
    test_file("1.0.0-bigendian", "generated_null_trivial")
}

#[test]
fn read_generated_100_null() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null")?;
    test_file("1.0.0-bigendian", "generated_null")
}

#[test]
fn read_generated_100_primitive_zerolength() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive_zerolength")?;
    test_file("1.0.0-bigendian", "generated_primitive_zerolength")
}

#[test]
fn read_generated_100_primitive_primitive_no_batches() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive_no_batches")?;
    test_file("1.0.0-bigendian", "generated_primitive_no_batches")
}

#[test]
fn read_generated_100_dictionary() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary")?;
    test_file("1.0.0-bigendian", "generated_dictionary")
}

#[test]
fn read_100_custom_metadata() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_custom_metadata")?;
    test_file("1.0.0-bigendian", "generated_custom_metadata")
}

#[test]
fn read_generated_100_nested_large_offsets() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_nested_large_offsets")?;
    test_file("1.0.0-bigendian", "generated_nested_large_offsets")
}

#[test]
fn read_generated_100_nested() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_nested")?;
    test_file("1.0.0-bigendian", "generated_nested")
}

#[test]
fn read_generated_100_dictionary_unsigned() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary_unsigned")?;
    test_file("1.0.0-bigendian", "generated_dictionary_unsigned")
}

#[test]
fn read_generated_100_decimal() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_decimal")?;
    test_file("1.0.0-bigendian", "generated_decimal")
}

#[test]
fn read_generated_100_interval() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_interval")?;
    test_file("1.0.0-bigendian", "generated_interval")
}

#[test]
fn read_generated_100_union() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_union")?;
    test_file("1.0.0-bigendian", "generated_union")
}

#[test]
fn read_generated_100_extension() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_extension")
}

#[test]
fn read_generated_100_map() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_map")?;
    test_file("1.0.0-bigendian", "generated_map")
}

#[test]
fn read_generated_100_non_canonical_map() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_map_non_canonical")?;
    test_file("1.0.0-bigendian", "generated_map_non_canonical")
}

#[test]
fn read_generated_100_nested_dictionary() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_nested_dictionary")?;
    test_file("1.0.0-bigendian", "generated_nested_dictionary")
}

#[test]
fn read_generated_017_union() -> Result<()> {
    test_file("0.17.1", "generated_union")
}

#[test]
#[cfg_attr(miri, ignore)] // LZ4 uses foreign calls that miri does not support
fn read_generated_200_compression_lz4() -> Result<()> {
    test_file("2.0.0-compression", "generated_lz4")
}

#[test]
#[cfg_attr(miri, ignore)] // ZSTD uses foreign calls that miri does not support
fn read_generated_200_compression_zstd() -> Result<()> {
    test_file("2.0.0-compression", "generated_zstd")
}

fn test_projection(version: &str, file_name: &str, columns: Vec<usize>) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
        testdata, version, file_name
    ))?;

    let metadata = read_file_metadata(&mut file)?;

    let expected = columns
        .iter()
        .copied()
        .map(|x| metadata.schema.fields[x].clone())
        .collect::<Vec<_>>();

    let mut reader = FileReader::new(&mut file, metadata, Some(columns));

    assert_eq!(reader.schema().fields, expected);

    reader.try_for_each(|rhs| {
        assert_eq!(rhs?.arrays().len(), expected.len());
        Result::Ok(())
    })?;
    Ok(())
}

#[test]
fn read_projected() -> Result<()> {
    test_projection("1.0.0-littleendian", "generated_primitive", vec![1])?;
    test_projection("1.0.0-littleendian", "generated_dictionary", vec![2])?;
    test_projection("1.0.0-littleendian", "generated_nested", vec![0])?;

    test_projection("1.0.0-littleendian", "generated_primitive", vec![2, 1])
}

fn read_corrupted_ipc(data: Vec<u8>) -> Result<()> {
    let mut file = std::io::Cursor::new(data);

    let metadata = read_file_metadata(&mut file)?;
    let mut reader = FileReader::new(file, metadata, None);

    reader.try_for_each(|rhs| {
        rhs?;
        Result::Ok(())
    })?;

    Ok(())
}

#[test]
fn test_does_not_panic() {
    use rand::Rng; // 0.8.0

    let version = "1.0.0-littleendian";
    let file_name = "generated_primitive";
    let testdata = crate::test_util::arrow_test_data();
    let path = format!(
        "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
        testdata, version, file_name
    );
    let original = std::fs::read(path).unwrap();

    for _ in 0..1000 {
        let mut data = original.clone();
        let position: usize = rand::thread_rng().gen_range(0..data.len());
        let new_byte: u8 = rand::thread_rng().gen_range(0..u8::MAX);
        data[position] = new_byte;
        let _ = read_corrupted_ipc(data);
    }
}
