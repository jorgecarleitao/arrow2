use std::sync::Arc;

use avro_rs::types::Record;
use avro_rs::Schema as AvroSchema;
use avro_rs::Writer;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::read;
use arrow2::record_batch::RecordBatch;

fn schema() -> (AvroSchema, Schema) {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "int"},
            {"name": "d", "type": "bytes"},
            {"name": "e", "type": "double"},
            {"name": "f", "type": "boolean"}
        ]
    }
"#;

    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Binary, false),
        Field::new("e", DataType::Float64, false),
        Field::new("f", DataType::Boolean, false),
    ]);

    (AvroSchema::parse_str(raw_schema).unwrap(), schema)
}

fn write() -> Result<(Vec<u8>, RecordBatch)> {
    let (avro, schema) = schema();
    // a writer needs a schema and something to write to
    let mut writer = Writer::new(&avro, Vec::new());

    // the Record type models our Record schema
    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");
    record.put("c", 1i32);
    record.put("d", b"foo".as_ref());
    record.put("e", 1.0f64);
    record.put("f", true);
    writer.append(record)?;

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("b", "bar");
    record.put("a", 47i64);
    record.put("c", 1i32);
    record.put("d", b"bar".as_ref());
    record.put("e", 2.0f64);
    record.put("f", false);
    writer.append(record)?;

    let columns = vec![
        Arc::new(Int64Array::from_slice([27, 47])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from_slice(["foo", "bar"])) as Arc<dyn Array>,
        Arc::new(Int32Array::from_slice([1, 1])) as Arc<dyn Array>,
        Arc::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])) as Arc<dyn Array>,
        Arc::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0])) as Arc<dyn Array>,
        Arc::new(BooleanArray::from_slice([true, false])) as Arc<dyn Array>,
    ];

    let expected = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    Ok((writer.into_inner().unwrap(), expected))
}

#[test]
fn read() -> Result<()> {
    let (data, expected) = write()?;

    let file = &mut &data[..];

    let (schema, codec, file_marker) = read::read_metadata(file)?;

    let mut reader = read::Reader::new(
        read::Decompressor::new(read::BlockStreamIterator::new(file, file_marker), codec),
        Arc::new(schema),
    );

    assert_eq!(reader.next().unwrap().unwrap(), expected);
    Ok(())
}
