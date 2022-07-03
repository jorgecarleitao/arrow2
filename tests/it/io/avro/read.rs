use arrow2::chunk::Chunk;
use avro_rs::types::{Record, Value};
use avro_rs::{Codec, Writer};
use avro_rs::{Days, Decimal, Duration, Millis, Months, Schema as AvroSchema};

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::avro::read;

pub(super) fn schema() -> (AvroSchema, Schema) {
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
            {"name": "i", "type": {
                "type": "record",
                "name": "bla",
                "fields": [
                    {"name": "e", "type": "double"}
                ]
            }},
            {"name": "enum", "type": {
                "type": "enum",
                "name": "",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }},
            {"name": "decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 5}}
        ]
    }
"#;

    let schema = Schema::from(vec![
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
            "i",
            DataType::Struct(vec![Field::new("e", DataType::Float64, false)]),
            false,
        ),
        Field::new(
            "enum",
            DataType::Dictionary(i32::KEY_TYPE, Box::new(DataType::Utf8), false),
            false,
        ),
        Field::new("decimal", DataType::Decimal(18, 5), false),
    ]);

    (AvroSchema::parse_str(raw_schema).unwrap(), schema)
}

pub(super) fn data() -> Chunk<Box<dyn Array>> {
    let data = vec![
        Some(vec![Some(1i32), None, Some(3)]),
        Some(vec![Some(1i32), None, Some(3)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();

    let columns = vec![
        Int64Array::from_slice([27, 47]).boxed(),
        Utf8Array::<i32>::from_slice(["foo", "bar"]).boxed(),
        Int32Array::from_slice([1, 1]).boxed(),
        Int32Array::from_slice([1, 2]).to(DataType::Date32).boxed(),
        BinaryArray::<i32>::from_slice([b"foo", b"bar"]).boxed(),
        PrimitiveArray::<f64>::from_slice([1.0, 2.0]).boxed(),
        BooleanArray::from_slice([true, false]).boxed(),
        Utf8Array::<i32>::from([Some("foo"), None]).boxed(),
        array.into_box(),
        StructArray::from_data(
            DataType::Struct(vec![Field::new("e", DataType::Float64, false)]),
            vec![Box::new(PrimitiveArray::<f64>::from_slice([1.0, 2.0]))],
            None,
        )
        .boxed(),
        DictionaryArray::try_from_keys(
            Int32Array::from_slice([1, 0]),
            Box::new(Utf8Array::<i32>::from_slice(["SPADES", "HEARTS"])),
        )
        .unwrap()
        .boxed(),
        PrimitiveArray::<i128>::from_slice([12345678i128, -12345678i128])
            .to(DataType::Decimal(18, 5))
            .boxed(),
    ];

    Chunk::try_new(columns).unwrap()
}

pub(super) fn write_avro(codec: Codec) -> std::result::Result<Vec<u8>, avro_rs::Error> {
    let (avro, _) = schema();
    // a writer needs a schema and something to write to
    let mut writer = Writer::with_codec(&avro, Vec::new(), codec);

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
    record.put(
        "i",
        Value::Record(vec![("e".to_string(), Value::Double(1.0f64))]),
    );
    record.put("enum", Value::Enum(1, "HEARTS".to_string()));
    record.put(
        "decimal",
        Value::Decimal(Decimal::from(&[0u8, 188u8, 97u8, 78u8])),
    );
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
    record.put(
        "i",
        Value::Record(vec![("e".to_string(), Value::Double(2.0f64))]),
    );
    record.put("enum", Value::Enum(0, "SPADES".to_string()));
    record.put(
        "decimal",
        Value::Decimal(Decimal::from(&[
            255u8, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 67, 158, 178,
        ])),
    );
    writer.append(record)?;
    Ok(writer.into_inner().unwrap())
}

pub(super) fn read_avro(
    mut avro: &[u8],
    projection: Option<Vec<bool>>,
) -> Result<(Chunk<Box<dyn Array>>, Schema)> {
    let file = &mut avro;

    let (avro_schema, schema, codec, file_marker) = read::read_metadata(file)?;

    let mut reader = read::Reader::new(
        read::Decompressor::new(read::BlockStreamIterator::new(file, file_marker), codec),
        avro_schema,
        schema.fields.clone(),
        projection.clone(),
    );

    let schema = if let Some(projection) = projection {
        let fields = schema
            .fields
            .into_iter()
            .zip(projection.iter())
            .filter_map(|x| if *x.1 { Some(x.0) } else { None })
            .collect::<Vec<_>>();
        Schema::from(fields)
    } else {
        schema
    };

    reader.next().unwrap().map(|x| (x, schema))
}

fn test(codec: Codec) -> Result<()> {
    let avro = write_avro(codec).unwrap();
    let expected = data();
    let (_, expected_schema) = schema();

    let (result, schema) = read_avro(&avro, None)?;

    assert_eq!(schema, expected_schema);
    assert_eq!(result, expected);
    Ok(())
}

#[test]
fn read_without_codec() -> Result<()> {
    test(Codec::Null)
}

#[cfg(feature = "io_avro_compression")]
#[test]
fn read_deflate() -> Result<()> {
    test(Codec::Deflate)
}

#[cfg(feature = "io_avro_compression")]
#[test]
fn read_snappy() -> Result<()> {
    test(Codec::Snappy)
}

#[test]
fn test_projected() -> Result<()> {
    let expected = data();
    let (_, expected_schema) = schema();

    let avro = write_avro(Codec::Null).unwrap();

    for i in 0..expected_schema.fields.len() {
        let mut projection = vec![false; expected_schema.fields.len()];
        projection[i] = true;

        let expected = expected
            .clone()
            .into_arrays()
            .into_iter()
            .zip(projection.iter())
            .filter_map(|x| if *x.1 { Some(x.0) } else { None })
            .collect();
        let expected = Chunk::new(expected);

        let expected_fields = expected_schema
            .clone()
            .fields
            .into_iter()
            .zip(projection.iter())
            .filter_map(|x| if *x.1 { Some(x.0) } else { None })
            .collect::<Vec<_>>();
        let expected_schema = Schema::from(expected_fields);

        let (result, schema) = read_avro(&avro, Some(projection))?;

        assert_eq!(schema, expected_schema);
        assert_eq!(result, expected);
    }
    Ok(())
}
