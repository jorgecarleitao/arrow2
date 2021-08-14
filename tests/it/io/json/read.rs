use flate2::read::GzDecoder;
use std::io::BufReader;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};
use std::{io::Cursor, sync::Arc};

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::{bitmap::Bitmap, buffer::Buffer, error::Result, io::json::*};

#[test]
fn test_json_basic() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/basic.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(4, batch.num_columns());
    assert_eq!(12, batch.num_rows());

    let schema = reader.schema();
    let batch_schema = batch.schema();
    assert_eq!(schema, batch_schema);

    let a = schema.column_with_name("a").unwrap();
    assert_eq!(0, a.0);
    assert_eq!(&DataType::Int64, a.1.data_type());
    let b = schema.column_with_name("b").unwrap();
    assert_eq!(1, b.0);
    assert_eq!(&DataType::Float64, b.1.data_type());
    let c = schema.column_with_name("c").unwrap();
    assert_eq!(2, c.0);
    assert_eq!(&DataType::Boolean, c.1.data_type());
    let d = schema.column_with_name("d").unwrap();
    assert_eq!(3, d.0);
    assert_eq!(&DataType::Utf8, d.1.data_type());

    let aa = batch
        .column(a.0)
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    assert_eq!(1, aa.value(0));
    assert_eq!(-10, aa.value(1));
    let bb = batch
        .column(b.0)
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    assert!((2.0 - bb.value(0)).abs() < f64::EPSILON);
    assert!((-3.5 - bb.value(1)).abs() < f64::EPSILON);
    let cc = batch
        .column(c.0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!cc.value(0));
    assert!(cc.value(10));
    let dd = batch
        .column(d.0)
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .unwrap();
    assert_eq!("4", dd.value(0));
    assert_eq!("text", dd.value(8));
}

#[test]
fn test_json_basic_with_nulls() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(4, batch.num_columns());
    assert_eq!(12, batch.num_rows());

    let schema = reader.schema();
    let batch_schema = batch.schema();
    assert_eq!(schema, batch_schema);

    let a = schema.column_with_name("a").unwrap();
    assert_eq!(&DataType::Int64, a.1.data_type());
    let b = schema.column_with_name("b").unwrap();
    assert_eq!(&DataType::Float64, b.1.data_type());
    let c = schema.column_with_name("c").unwrap();
    assert_eq!(&DataType::Boolean, c.1.data_type());
    let d = schema.column_with_name("d").unwrap();
    assert_eq!(&DataType::Utf8, d.1.data_type());

    let aa = batch
        .column(a.0)
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    assert!(aa.is_valid(0));
    assert!(!aa.is_valid(1));
    assert!(!aa.is_valid(11));
    let bb = batch
        .column(b.0)
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    assert!(bb.is_valid(0));
    assert!(!bb.is_valid(2));
    assert!(!bb.is_valid(11));
    let cc = batch
        .column(c.0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(cc.is_valid(0));
    assert!(!cc.is_valid(4));
    assert!(!cc.is_valid(11));
    let dd = batch
        .column(d.0)
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .unwrap();
    assert!(!dd.is_valid(0));
    assert!(dd.is_valid(1));
    assert!(!dd.is_valid(4));
    assert!(!dd.is_valid(11));
}

#[test]
fn test_json_basic_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Float32, false),
        Field::new("c", DataType::Boolean, false),
        Field::new("d", DataType::Utf8, false),
    ]));

    let mut reader: Reader<File> = Reader::new(
        File::open("test/data/basic.json").unwrap(),
        schema.clone(),
        1024,
        None,
    );
    let reader_schema = reader.schema();
    assert_eq!(reader_schema, &schema);
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(4, batch.num_columns());
    assert_eq!(12, batch.num_rows());

    let schema = batch.schema();

    let a = schema.column_with_name("a").unwrap();
    assert_eq!(&DataType::Int32, a.1.data_type());
    let b = schema.column_with_name("b").unwrap();
    assert_eq!(&DataType::Float32, b.1.data_type());
    let c = schema.column_with_name("c").unwrap();
    assert_eq!(&DataType::Boolean, c.1.data_type());
    let d = schema.column_with_name("d").unwrap();
    assert_eq!(&DataType::Utf8, d.1.data_type());

    let aa = batch
        .column(a.0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(1, aa.value(0));
    // test that a 64bit value is returned as null due to overflowing
    assert!(!aa.is_valid(11));
    let bb = batch
        .column(b.0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!((2.0 - bb.value(0)).abs() < f32::EPSILON);
    assert!((-3.5 - bb.value(1)).abs() < f32::EPSILON);
}

#[test]
fn test_json_basic_schema_projection() {
    // We test implicit and explicit projection:
    // Implicit: omitting fields from a schema
    // Explicit: supplying a vec of fields to take
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Float32, false),
        Field::new("c", DataType::Boolean, false),
    ]));

    let mut reader: Reader<File> = Reader::new(
        File::open("test/data/basic.json").unwrap(),
        schema,
        1024,
        Some(vec!["a".to_string(), "c".to_string()]),
    );
    let reader_schema = reader.schema().clone();
    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("c", DataType::Boolean, false),
    ]);
    assert_eq!(reader_schema.as_ref(), &expected_schema);

    let batch = reader.next().unwrap().unwrap();

    assert_eq!(2, batch.num_columns());
    assert_eq!(12, batch.num_rows());

    let batch_schema = batch.schema();
    assert_eq!(&reader_schema, batch_schema);

    let a = batch_schema.column_with_name("a").unwrap();
    assert_eq!(0, a.0);
    assert_eq!(&DataType::Int32, a.1.data_type());
    let c = batch_schema.column_with_name("c").unwrap();
    assert_eq!(1, c.0);
    assert_eq!(&DataType::Boolean, c.1.data_type());
}

#[test]
fn test_json_arrays() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/arrays.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(4, batch.num_columns());
    assert_eq!(3, batch.num_rows());

    let schema = batch.schema();

    let a = schema.column_with_name("a").unwrap();
    assert_eq!(&DataType::Int64, a.1.data_type());
    let b = schema.column_with_name("b").unwrap();
    assert_eq!(
        &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        b.1.data_type()
    );
    let c = schema.column_with_name("c").unwrap();
    assert_eq!(
        &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
        c.1.data_type()
    );
    let d = schema.column_with_name("d").unwrap();
    assert_eq!(&DataType::Utf8, d.1.data_type());

    let aa = batch
        .column(a.0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(1, aa.value(0));
    assert_eq!(-10, aa.value(1));
    let bb = batch
        .column(b.0)
        .as_any()
        .downcast_ref::<ListArray<i32>>()
        .unwrap();
    let bb = bb.values();
    let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(9, bb.len());
    assert!((2.0 - bb.value(0)).abs() < f64::EPSILON);
    assert!((-6.1 - bb.value(5)).abs() < f64::EPSILON);
    assert!(!bb.is_valid(7));

    let cc = batch
        .column(c.0)
        .as_any()
        .downcast_ref::<ListArray<i32>>()
        .unwrap();
    let cc = cc.values();
    let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(6, cc.len());
    assert!(!cc.value(0));
    assert!(!cc.value(4));
    assert!(!cc.is_valid(5));
}

#[test]
fn test_invalid_json_infer_schema() {
    let re = infer_json_schema_from_seekable(
        &mut BufReader::new(File::open("test/data/uk_cities_with_headers.csv").unwrap()),
        None,
    );
    assert_eq!(
        re.err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
}

#[test]
fn test_invalid_json_read_record() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
        true,
    )]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/uk_cities_with_headers.csv").unwrap())
        .unwrap();
    assert_eq!(
        reader.next().err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
}

#[test]
fn test_mixed_json_arrays() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/mixed_arrays.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
    let mut reader = BufReader::new(GzDecoder::new(&file));
    let schema = Arc::new(infer_json_schema(&mut reader, None).unwrap());
    file.seek(SeekFrom::Start(0)).unwrap();

    let reader = BufReader::new(GzDecoder::new(&file));
    let mut reader = Reader::from_buf_reader(reader, schema, 64, None);
    let batch_gz = reader.next().unwrap().unwrap();

    for batch in vec![batch, batch_gz] {
        assert_eq!(4, batch.num_columns());
        assert_eq!(4, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            c.1.data_type()
        );
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            d.1.data_type()
        );

        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        let bb = bb.values();
        let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(9, bb.len());
        assert!((-6.1 - bb.value(8)).abs() < f64::EPSILON);

        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        let cc = cc.values();
        let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
        let cc_expected = BooleanArray::from(vec![Some(false), Some(true), Some(false), None]);
        assert_eq!(cc, &cc_expected);

        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        let dd = dd.values();
        let dd = dd.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        assert_eq!(
            dd,
            &Utf8Array::<i32>::from_slice(&["1", "false", "array", "2.4"])
        );
    }
}

#[test]
fn test_nested_struct_json_arrays() {
    let d_field = Field::new("d", DataType::Utf8, true);
    let c_field = Field::new("c", DataType::Struct(vec![d_field.clone()]), true);
    let a_field = Field::new(
        "a",
        DataType::Struct(vec![
            Field::new("b", DataType::Boolean, true),
            c_field.clone(),
        ]),
        true,
    );
    let schema = Arc::new(Schema::new(vec![a_field]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/nested_structs.json").unwrap())
        .unwrap();

    // build expected output
    let d = Utf8Array::<i32>::from(&vec![Some("text"), None, Some("text"), None]);
    let c = StructArray::from_data(vec![d_field], vec![Arc::new(d)], None);

    let b = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
    let expected = StructArray::from_data(
        vec![Field::new("b", DataType::Boolean, true), c_field],
        vec![Arc::new(b), Arc::new(c)],
        None,
    );

    // compare `a` with result from json reader
    let batch = reader.next().unwrap().unwrap();
    let read = batch.column(0);
    assert_eq!(expected, read.as_ref());
}

#[test]
fn test_nested_list_json_arrays() {
    let d_field = Field::new("d", DataType::Utf8, true);
    let c_field = Field::new("c", DataType::Struct(vec![d_field.clone()]), true);
    let b_field = Field::new("b", DataType::Boolean, true);
    let a_struct_field = Field::new(
        "a",
        DataType::Struct(vec![b_field.clone(), c_field.clone()]),
        true,
    );
    let a_list_data_type = DataType::List(Box::new(a_struct_field));
    let a_field = Field::new("a", a_list_data_type.clone(), true);
    let schema = Arc::new(Schema::new(vec![a_field]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let json_content = r#"
    {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
    {"a": [{"b": false, "c": null}]}
    {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
    {"a": null}
    {"a": []}
    "#;
    let mut reader = builder.build(Cursor::new(json_content)).unwrap();

    // build expected output
    let d = Utf8Array::<i32>::from(&vec![
        Some("a_text"),
        Some("b_text"),
        None,
        Some("c_text"),
        Some("d_text"),
        None,
    ]);

    let c = StructArray::from_data(vec![d_field], vec![Arc::new(d)], None);

    let b = BooleanArray::from(vec![
        Some(true),
        Some(false),
        Some(false),
        Some(true),
        None,
        Some(true),
    ]);
    let a_struct = StructArray::from_data(
        vec![b_field, c_field],
        vec![Arc::new(b) as Arc<dyn Array>, Arc::new(c) as Arc<dyn Array>],
        None,
    );
    let expected = ListArray::from_data(
        a_list_data_type,
        Buffer::from([0i32, 2, 3, 6, 6, 6]),
        Arc::new(a_struct) as Arc<dyn Array>,
        Some(Bitmap::from_u8_slice([0b00010111], 5)),
    );

    // compare `a` with result from json reader
    let batch = reader.next().unwrap().unwrap();
    let read = batch.column(0);
    assert_eq!(expected, read.as_ref());
}

#[test]
fn test_dictionary_from_json_basic_with_nulls() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
    )]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(1, batch.num_columns());
    assert_eq!(12, batch.num_rows());

    let schema = reader.schema();
    let batch_schema = batch.schema();
    assert_eq!(schema, batch_schema);

    let d = schema.column_with_name("d").unwrap();
    let data_type = DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
    assert_eq!(&data_type, d.1.data_type());

    let result = batch.column(d.0);

    let values = vec![
        None,
        Some("4"),
        Some("text"),
        Some("4"),
        None,
        None,
        Some("4"),
        None,
        Some("text"),
        Some("4"),
        Some("4"),
        None,
    ];

    let mut expected = MutableDictionaryArray::<i16, MutableUtf8Array<i32>>::new();
    expected.try_extend(values)?;
    let expected: DictionaryArray<i16> = expected.into();

    assert_eq!(expected, result.as_ref());
    Ok(())
}

#[test]
fn test_skip_empty_lines() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let json_content = "
    {\"a\": 1}

    {\"a\": 2}

    {\"a\": 3}";
    let mut reader = builder.build(Cursor::new(json_content)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(1, batch.num_columns());
    assert_eq!(3, batch.num_rows());

    let schema = reader.schema();
    let c = schema.column_with_name("a").unwrap();
    assert_eq!(&DataType::Int64, c.1.data_type());
}

#[test]
fn test_row_type_validation() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let json_content = "
    [1, \"hello\"]
    \"world\"";
    let re = builder.build(Cursor::new(json_content));
    assert_eq!(
        re.err().unwrap().to_string(),
        r#"Expected JSON record to be an object, found Array([Number(1), String("hello")])"#,
    );
}

#[test]
fn test_list_of_string_dictionary_from_json_with_nulls() -> Result<()> {
    let data_type = DataType::List(Box::new(Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
        true,
    )));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "events",
        data_type.clone(),
        true,
    )]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/list_string_dict_nested_nulls.json").unwrap())
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(1, batch.num_columns());
    assert_eq!(3, batch.num_rows());

    let schema = reader.schema();
    let batch_schema = batch.schema();
    assert_eq!(schema, batch_schema);

    let events = schema.column_with_name("events").unwrap();
    assert_eq!(&data_type, events.1.data_type());

    let expected = vec![
        Some(vec![None, Some("Elect Leader"), Some("Do Ballot")]),
        Some(vec![
            Some("Do Ballot"),
            None,
            Some("Send Data"),
            Some("Elect Leader"),
        ]),
        Some(vec![Some("Send Data")]),
    ];

    type A = MutableDictionaryArray<u64, MutableUtf8Array<i32>>;

    let mut array = MutableListArray::<i32, A>::new();
    array.try_extend(expected)?;

    let expected: ListArray<i32> = array.into();

    assert_eq!(expected, batch.column(0).as_ref());
    Ok(())
}

#[test]
fn test_with_multiple_batches() {
    let builder = ReaderBuilder::new()
        .infer_schema(Some(4))
        .with_batch_size(5);
    let mut reader: Reader<File> = builder
        .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
        .unwrap();

    let mut num_records = Vec::new();
    while let Some(rb) = reader.next().unwrap() {
        num_records.push(rb.num_rows());
    }

    assert_eq!(vec![5, 5, 2], num_records);
}

#[test]
fn test_json_infer_schema() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new(
            "b",
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        Field::new(
            "c",
            DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "d",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);

    let mut reader = BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
    let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();

    assert_eq!(inferred_schema, schema);

    let file = File::open("test/data/mixed_arrays.json.gz").unwrap();
    let mut reader = BufReader::new(GzDecoder::new(&file));
    let inferred_schema = infer_json_schema(&mut reader, None).unwrap();

    assert_eq!(inferred_schema, schema);
}
