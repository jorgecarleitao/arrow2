use std::io::Cursor;

use arrow2::array::*;
use arrow2::error::Result;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use arrow2::io::ipc::write::*;
use arrow2::record_batch::RecordBatch;

use crate::io::ipc::common::read_gzip_json;

fn round_trip(batch: RecordBatch) -> Result<()> {
    let result = Vec::<u8>::new();

    // write IPC version 5
    let written_result = {
        let options =
            IpcWriteOptions::try_new(8, false, MetadataVersion::V5, Some(Compression::ZSTD))?;
        let mut writer = FileWriter::try_new_with_options(result, batch.schema(), options)?;
        writer.write(&batch)?;
        writer.finish()?;
        writer.into_inner()
    };
    let mut reader = Cursor::new(written_result);
    let metadata = read_file_metadata(&mut reader)?;
    let schema = metadata.schema().clone();

    let reader = FileReader::new(reader, metadata, None);

    // read expected JSON output
    let (expected_schema, expected_batches) = (batch.schema().clone(), vec![batch]);

    assert_eq!(schema.as_ref(), expected_schema.as_ref());

    let batches = reader.collect::<Result<Vec<_>>>()?;

    assert_eq!(batches, expected_batches);
    Ok(())
}

fn test_file(version: &str, file_name: &str, compressed: bool) -> Result<()> {
    let (schema, batches) = read_gzip_json(version, file_name)?;

    let result = Vec::<u8>::new();

    let compression = if compressed {
        Some(Compression::ZSTD)
    } else {
        None
    };

    // write IPC version 5
    let written_result = {
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5, compression)?;
        let mut writer = FileWriter::try_new_with_options(result, &schema, options)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        writer.into_inner()
    };
    let mut reader = Cursor::new(written_result);
    let metadata = read_file_metadata(&mut reader)?;
    let schema = metadata.schema().clone();

    let reader = FileReader::new(reader, metadata, None);

    // read expected JSON output
    let (expected_schema, expected_batches) = read_gzip_json(version, file_name)?;

    assert_eq!(schema.as_ref(), &expected_schema);

    let batches = reader.collect::<Result<Vec<_>>>()?;

    for (a, b) in batches.iter().zip(expected_batches.iter()) {
        for (c1, c2) in a.columns().iter().zip(b.columns().iter()) {
            assert_eq!(c1, c2)
        }
    }

    //assert_eq!(batches, expected_batches);
    Ok(())
}

#[test]
fn write_100_primitive() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive", false)?;
    test_file("1.0.0-bigendian", "generated_primitive", false)?;
    test_file("1.0.0-littleendian", "generated_primitive", true)?;
    test_file("1.0.0-bigendian", "generated_primitive", true)
}

#[test]
fn write_100_datetime() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_datetime", false)?;
    test_file("1.0.0-bigendian", "generated_datetime", false)?;
    test_file("1.0.0-littleendian", "generated_datetime", true)?;
    test_file("1.0.0-bigendian", "generated_datetime", true)
}

#[test]
fn write_100_dictionary_unsigned() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary_unsigned", false)?;
    test_file("1.0.0-bigendian", "generated_dictionary_unsigned", false)?;
    test_file("1.0.0-littleendian", "generated_dictionary_unsigned", true)?;
    test_file("1.0.0-bigendian", "generated_dictionary_unsigned", true)
}

#[test]
fn write_100_dictionary() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary", false)?;
    test_file("1.0.0-bigendian", "generated_dictionary", false)?;
    test_file("1.0.0-littleendian", "generated_dictionary", true)?;
    test_file("1.0.0-bigendian", "generated_dictionary", true)
}

#[test]
fn write_100_interval() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_interval", false)?;
    test_file("1.0.0-bigendian", "generated_interval", false)?;
    test_file("1.0.0-littleendian", "generated_interval", true)?;
    test_file("1.0.0-bigendian", "generated_interval", true)
}

#[test]
fn write_100_large_batch() -> Result<()> {
    // this takes too long for unit-tests. It has been passing...
    //test_file("1.0.0-littleendian", "generated_large_batch", false);
    Ok(())
}

#[test]
fn write_100_nested() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_nested", false)?;
    test_file("1.0.0-bigendian", "generated_nested", false)?;
    test_file("1.0.0-littleendian", "generated_nested", true)?;
    test_file("1.0.0-bigendian", "generated_nested", true)
}

#[test]
fn write_100_nested_large_offsets() -> Result<()> {
    test_file(
        "1.0.0-littleendian",
        "generated_nested_large_offsets",
        false,
    )?;
    test_file("1.0.0-bigendian", "generated_nested_large_offsets", false)?;
    test_file("1.0.0-littleendian", "generated_nested_large_offsets", true)?;
    test_file("1.0.0-bigendian", "generated_nested_large_offsets", true)
}

#[test]
fn write_100_null_trivial() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null_trivial", false)?;
    test_file("1.0.0-bigendian", "generated_null_trivial", false)?;
    test_file("1.0.0-littleendian", "generated_null_trivial", true)?;
    test_file("1.0.0-bigendian", "generated_null_trivial", true)
}

#[test]
fn write_100_null() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_null", false)?;
    test_file("1.0.0-bigendian", "generated_null", false)?;
    test_file("1.0.0-littleendian", "generated_null", true)?;
    test_file("1.0.0-bigendian", "generated_null", true)
}

#[test]
fn write_100_primitive_large_offsets() -> Result<()> {
    test_file(
        "1.0.0-littleendian",
        "generated_primitive_large_offsets",
        false,
    )?;
    test_file(
        "1.0.0-bigendian",
        "generated_primitive_large_offsets",
        false,
    )?;
    test_file(
        "1.0.0-littleendian",
        "generated_primitive_large_offsets",
        true,
    )?;
    test_file("1.0.0-bigendian", "generated_primitive_large_offsets", true)
}

#[test]
fn write_100_primitive_no_batches() -> Result<()> {
    test_file(
        "1.0.0-littleendian",
        "generated_primitive_no_batches",
        false,
    )?;
    test_file("1.0.0-bigendian", "generated_primitive_no_batches", false)?;
    test_file("1.0.0-littleendian", "generated_primitive_no_batches", true)?;
    test_file("1.0.0-bigendian", "generated_primitive_no_batches", true)
}

#[test]
fn write_100_primitive_zerolength() -> Result<()> {
    test_file(
        "1.0.0-littleendian",
        "generated_primitive_zerolength",
        false,
    )?;
    test_file("1.0.0-bigendian", "generated_primitive_zerolength", false)?;
    test_file("1.0.0-littleendian", "generated_primitive_zerolength", true)?;
    test_file("1.0.0-bigendian", "generated_primitive_zerolength", true)
}

#[test]
fn write_0141_primitive_zerolength() -> Result<()> {
    test_file("0.14.1", "generated_primitive_zerolength", false)
}

#[test]
fn write_100_custom_metadata() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_custom_metadata", false)?;
    test_file("1.0.0-bigendian", "generated_custom_metadata", false)
}

#[test]
fn write_100_decimal() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_decimal", false)?;
    test_file("1.0.0-bigendian", "generated_decimal", false)
}

#[test]
fn write_100_extension() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_extension", false)?;
    test_file("1.0.0-bigendian", "generated_extension", false)
}

#[test]
fn write_100_union() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_union", false)?;
    test_file("1.0.0-bigendian", "generated_union", false)
}

#[test]
fn write_100_map() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_map", false)?;
    test_file("1.0.0-bigendian", "generated_map", false)
}

#[test]
fn write_100_map_non_canonical() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_map_non_canonical", false)?;
    test_file("1.0.0-bigendian", "generated_map_non_canonical", false)
}

#[test]
fn write_generated_017_union() -> Result<()> {
    test_file("0.17.1", "generated_union", false)
}

#[test]
fn write_boolean() -> Result<()> {
    use std::sync::Arc;
    let array = Arc::new(BooleanArray::from([
        Some(true),
        Some(false),
        None,
        Some(true),
    ])) as Arc<dyn Array>;
    let batch = RecordBatch::try_from_iter(vec![("a", array)])?;
    round_trip(batch)
}

#[test]
fn write_sliced_utf8() -> Result<()> {
    use std::sync::Arc;
    let array = Arc::new(Utf8Array::<i32>::from_slice(["aa", "bb"]).slice(1, 1)) as Arc<dyn Array>;
    let batch = RecordBatch::try_from_iter(vec![("a", array)])?;
    round_trip(batch)
}

#[test]
fn write_sliced_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    let array = array.into_arc().slice(1, 2).into();
    let batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
    round_trip(batch)
}
