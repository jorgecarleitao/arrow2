use std::io::Cursor;

use arrow2::error::Result;
use arrow2::io::ipc::read::read_stream_metadata;
use arrow2::io::ipc::read::StreamReader;
use arrow2::io::ipc::write::{StreamWriter, WriteOptions};

use crate::io::ipc::common::read_arrow_stream;
use crate::io::ipc::common::read_gzip_json;

fn test_file(version: &str, file_name: &str) {
    let (schema, batches) = read_arrow_stream(version, file_name);

    let mut result = Vec::<u8>::new();

    // write IPC version 5
    {
        let options = WriteOptions { compression: None };
        let mut writer = StreamWriter::try_new(&mut result, &schema, options).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    let mut reader = Cursor::new(result);
    let metadata = read_stream_metadata(&mut reader).unwrap();
    let reader = StreamReader::new(reader, metadata);

    let schema = reader.schema().clone();

    // read expected JSON output
    let (expected_schema, expected_batches) = read_gzip_json(version, file_name).unwrap();

    assert_eq!(schema.as_ref(), &expected_schema);

    let batches = reader
        .map(|x| x.map(|x| x.unwrap()))
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(batches, expected_batches);
}

#[test]
fn write_100_primitive() {
    test_file("1.0.0-littleendian", "generated_primitive");
}

#[test]
fn write_100_datetime() {
    test_file("1.0.0-littleendian", "generated_datetime");
}

#[test]
fn write_100_dictionary_unsigned() {
    test_file("1.0.0-littleendian", "generated_dictionary_unsigned");
}

#[test]
fn write_100_dictionary() {
    test_file("1.0.0-littleendian", "generated_dictionary");
}

#[test]
fn write_100_interval() {
    test_file("1.0.0-littleendian", "generated_interval");
}

#[test]
fn write_100_large_batch() {
    // this takes too long for unit-tests. It has been passing...
    //test_file("1.0.0-littleendian", "generated_large_batch");
}

#[test]
fn write_100_nested() {
    test_file("1.0.0-littleendian", "generated_nested");
}

#[test]
fn write_100_nested_large_offsets() {
    test_file("1.0.0-littleendian", "generated_nested_large_offsets");
}

#[test]
fn write_100_null_trivial() {
    test_file("1.0.0-littleendian", "generated_null_trivial");
}

#[test]
fn write_100_null() {
    test_file("1.0.0-littleendian", "generated_null");
}

#[test]
fn write_100_primitive_large_offsets() {
    test_file("1.0.0-littleendian", "generated_primitive_large_offsets");
}

#[test]
fn write_100_union() {
    test_file("1.0.0-littleendian", "generated_union");
}

#[test]
fn write_generated_017_union() {
    test_file("0.17.1", "generated_union");
}

//#[test]
//fn write_100_recursive_nested() {
//test_file("1.0.0-littleendian", "generated_recursive_nested");
//}

#[test]
fn write_100_primitive_no_batches() {
    test_file("1.0.0-littleendian", "generated_primitive_no_batches");
}

#[test]
fn write_100_primitive_zerolength() {
    test_file("1.0.0-littleendian", "generated_primitive_zerolength");
}

#[test]
fn write_100_custom_metadata() {
    test_file("1.0.0-littleendian", "generated_custom_metadata");
}

#[test]
fn write_100_decimal() {
    test_file("1.0.0-littleendian", "generated_decimal");
}
