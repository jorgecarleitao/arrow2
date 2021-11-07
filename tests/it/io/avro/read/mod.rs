use std::sync::Arc;

use arrow2::types::months_days_ns;
use avro_rs::types::{Record, Value};
use avro_rs::{Codec, Writer};
use avro_rs::{Days, Duration, Millis, Months, Schema as AvroSchema};

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
            {
                "name": "date",
                "type": "int",
                "logicalType": "date"
            },
            {"name": "d", "type": "bytes"},
            {"name": "e", "type": "double"},
            {"name": "f", "type": "boolean"},
            {"name": "g", "type": ["null", "string"], "default": null},
            {"name": "h", "type": {
                "type": "array",
                "items": {
                    "name": "item",
                    "type": ["null", "int"],
                    "default": null
                }
            }},
            {"name": "enum", "type": {
                "type": "enum",
                "name": "",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }},
            {"name": "duration",
             "logicalType": "duration",
             "type": {
                "name": "duration",
                "type": "fixed",
                "size": 12
            }}
        ]
    }
"#;

    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int32, false),
        Field::new("date", DataType::Date32, false),
        Field::new("d", DataType::Binary, false),
        Field::new("e", DataType::Float64, false),
        Field::new("f", DataType::Boolean, false),
        Field::new("g", DataType::Utf8, true),
        Field::new(
            "h",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
        Field::new(
            "enum",
            DataType::Dictionary(i32::KEY_TYPE, Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "duration",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]);

    (AvroSchema::parse_str(raw_schema).unwrap(), schema)
}

fn write(has_codec: bool) -> Result<(Vec<u8>, RecordBatch)> {
    let (avro, schema) = schema();
    // a writer needs a schema and something to write to
    let mut writer: Writer<Vec<u8>>;
    if has_codec {
        writer = Writer::with_codec(&avro, Vec::new(), Codec::Deflate);
    } else {
        writer = Writer::new(&avro, Vec::new());
    }

    // the Record type models our Record schema
    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");
    record.put("c", 1i32);
    record.put("date", 1i32);
    record.put("d", b"foo".as_ref());
    record.put("e", 1.0f64);
    record.put("f", true);
    record.put("g", Some("foo"));
    record.put(
        "h",
        Value::Array(vec![
            Value::Union(Box::new(Value::Int(1))),
            Value::Union(Box::new(Value::Null)),
            Value::Union(Box::new(Value::Int(3))),
        ]),
    );
    record.put("enum", Value::Enum(1, "HEARTS".to_string()));
    record.put(
        "duration",
        Value::Duration(Duration::new(Months::new(1), Days::new(1), Millis::new(1))),
    );
    writer.append(record)?;

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("b", "bar");
    record.put("a", 47i64);
    record.put("c", 1i32);
    record.put("date", 2i32);
    record.put("d", b"bar".as_ref());
    record.put("e", 2.0f64);
    record.put("f", false);
    record.put("g", None::<&str>);
    record.put(
        "h",
        Value::Array(vec![
            Value::Union(Box::new(Value::Int(1))),
            Value::Union(Box::new(Value::Null)),
            Value::Union(Box::new(Value::Int(3))),
        ]),
    );
    record.put("enum", Value::Enum(0, "SPADES".to_string()));
    record.put(
        "duration",
        Value::Duration(Duration::new(Months::new(1), Days::new(2), Millis::new(1))),
    );
    writer.append(record)?;

    let data = vec![
        Some(vec![Some(1i32), None, Some(3)]),
        Some(vec![Some(1i32), None, Some(3)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();

    let columns = vec![
        Arc::new(Int64Array::from_slice([27, 47])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from_slice(["foo", "bar"])) as Arc<dyn Array>,
        Arc::new(Int32Array::from_slice([1, 1])) as Arc<dyn Array>,
        Arc::new(Int32Array::from_slice([1, 2]).to(DataType::Date32)) as Arc<dyn Array>,
        Arc::new(BinaryArray::<i32>::from_slice([b"foo", b"bar"])) as Arc<dyn Array>,
        Arc::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0])) as Arc<dyn Array>,
        Arc::new(BooleanArray::from_slice([true, false])) as Arc<dyn Array>,
        Arc::new(Utf8Array::<i32>::from([Some("foo"), None])) as Arc<dyn Array>,
        array.into_arc(),
        Arc::new(DictionaryArray::<i32>::from_data(
            Int32Array::from_slice([1, 0]),
            Arc::new(Utf8Array::<i32>::from_slice(["SPADES", "HEARTS"])),
        )) as Arc<dyn Array>,
        Arc::new(MonthsDaysNsArray::from_slice([
            months_days_ns::new(1, 1, 1_000_000),
            months_days_ns::new(1, 2, 1_000_000),
        ])) as Arc<dyn Array>,
    ];

    let expected = RecordBatch::try_new(Arc::new(schema), columns).unwrap();

    Ok((writer.into_inner().unwrap(), expected))
}

#[test]
fn read_without_codec() -> Result<()> {
    let (data, expected) = write(false).unwrap();

    let file = &mut &data[..];

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(file)?;

    let mut reader = read::Reader::new(
        read::Decompressor::new(read::BlockStreamIterator::new(file, file_marker), codec),
        avro_schema,
        Arc::new(schema),
    );

    assert_eq!(reader.next().unwrap().unwrap(), expected);
    Ok(())
}

#[test]
fn read_with_codec() -> Result<()> {
    let (data, expected) = write(true).unwrap();

    let file = &mut &data[..];

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(file)?;

    let mut reader = read::Reader::new(
        read::Decompressor::new(read::BlockStreamIterator::new(file, file_marker), codec),
        avro_schema,
        Arc::new(schema),
    );

    assert_eq!(reader.next().unwrap().unwrap(), expected);
    Ok(())
}
