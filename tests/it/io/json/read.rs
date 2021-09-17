use std::io::BufReader;
use std::{io::Cursor, sync::Arc};

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::{bitmap::Bitmap, buffer::Buffer, error::Result, io::json::*};

use crate::io::json::*;

#[test]
fn basic() -> Result<()> {
    let (data, schema, columns) = case_basics();

    let mut reader = ReaderBuilder::new().build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    assert_eq!(&schema, batch.schema().as_ref());

    columns
        .iter()
        .zip(batch.columns())
        .for_each(|(expected, result)| assert_eq!(expected.as_ref(), result.as_ref()));
    Ok(())
}

#[test]
fn basics_with_schema_projection() -> Result<()> {
    let (data, schema, columns) = case_basics_schema();

    let mut reader = ReaderBuilder::new()
        .with_schema(Arc::new(schema.clone()))
        .build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    assert_eq!(&schema, batch.schema().as_ref());

    columns
        .iter()
        .zip(batch.columns())
        .for_each(|(expected, result)| assert_eq!(expected.as_ref(), result.as_ref()));
    Ok(())
}

#[test]
fn lists() -> Result<()> {
    let (data, schema, columns) = case_list();

    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader = builder.build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    assert_eq!(&schema, batch.schema().as_ref());

    columns
        .iter()
        .zip(batch.columns())
        .for_each(|(expected, result)| assert_eq!(expected.as_ref(), result.as_ref()));
    Ok(())
}

#[test]
fn line_break_in_values() -> Result<()> {
    let data = r#"
    {"a":"aa\n\n"}
    {"a":"aa\n"}
    {"a":null}
    "#;

    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let mut reader = builder.build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    let expected = Utf8Array::<i32>::from(&[Some("aa\n\n"), Some("aa\n"), None]);

    assert_eq!(expected, batch.columns()[0].as_ref());
    Ok(())
}

#[test]
fn invalid_infer_schema() -> Result<()> {
    let re =
        infer_json_schema_from_seekable(&mut BufReader::new(Cursor::new("city,lat,lng")), None);
    assert_eq!(
        re.err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
    Ok(())
}

#[test]
fn invalid_read_record() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
        true,
    )]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let mut data = Cursor::new("city,lat,lng");
    let mut reader = builder.build(&mut data)?;
    assert_eq!(
        reader.next().err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
    Ok(())
}

#[test]
fn nested_struct_arrays() -> Result<()> {
    let (data, schema, columns) = case_struct();

    let builder = ReaderBuilder::new().with_schema(Arc::new(schema.clone()));
    let mut reader = builder.build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    assert_eq!(&schema, batch.schema().as_ref());

    columns
        .iter()
        .zip(batch.columns())
        .for_each(|(expected, result)| assert_eq!(expected.as_ref(), result.as_ref()));
    Ok(())
}

#[test]
fn nested_list_arrays() {
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
    let content = r#"
    {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
    {"a": [{"b": false, "c": null}]}
    {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
    {"a": null}
    {"a": []}
    "#;
    let mut reader = builder.build(Cursor::new(content)).unwrap();

    // build expected output
    let d = Utf8Array::<i32>::from(&vec![
        Some("a_text"),
        Some("b_text"),
        None,
        Some("c_text"),
        Some("d_text"),
        None,
    ]);

    let c = StructArray::from_data(DataType::Struct(vec![d_field]), vec![Arc::new(d)], None);

    let b = BooleanArray::from(vec![
        Some(true),
        Some(false),
        Some(false),
        Some(true),
        None,
        Some(true),
    ]);
    let a_struct = StructArray::from_data(
        DataType::Struct(vec![b_field, c_field]),
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
fn skip_empty_lines() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let content = "
    {\"a\": 1}

    {\"a\": 2}

    {\"a\": 3}";
    let mut reader = builder.build(Cursor::new(content)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(1, batch.num_columns());
    assert_eq!(3, batch.num_rows());

    let schema = reader.schema();
    let c = schema.column_with_name("a").unwrap();
    assert_eq!(&DataType::Int64, c.1.data_type());
}

#[test]
fn row_type_validation() {
    let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
    let content = "
    [1, \"hello\"]
    \"world\"";
    let re = builder.build(Cursor::new(content));
    assert_eq!(
        re.err().unwrap().to_string(),
        r#"Expected JSON record to be an object, found Array([Number(1), String("hello")])"#,
    );
}

#[test]
fn list_of_string_dictionary_from_with_nulls() -> Result<()> {
    let (data, schema, columns) = case_dict();

    let builder = ReaderBuilder::new()
        .with_schema(Arc::new(schema))
        .with_batch_size(64);
    let mut reader = builder.build(Cursor::new(data))?;
    let batch = reader.next()?.unwrap();

    assert_eq!(reader.schema(), batch.schema());

    assert_eq!(columns[0].as_ref(), batch.columns()[0].as_ref());
    Ok(())
}

#[test]
fn with_multiple_batches() -> Result<()> {
    let data = r#"
    {"a":1}
    {"a":null}
    {}
    {"a":1}
    {"a":7}
    {"a":1}
    {"a":1}
    {"a":5}
    {"a":1}
    {"a":1}
    {"a":1}
    {}
    "#;

    let builder = ReaderBuilder::new()
        .infer_schema(Some(4))
        .with_batch_size(5);
    let mut reader = builder.build(Cursor::new(data))?;

    let mut num_records = Vec::new();
    while let Some(rb) = reader.next()? {
        num_records.push(rb.num_rows());
    }

    assert_eq!(vec![5, 5, 2], num_records);
    Ok(())
}

#[test]
fn infer_schema_mixed_list() -> Result<()> {
    let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
    {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
    {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
    {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
    "#;

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

    let inferred_schema = infer_json_schema(&mut BufReader::new(Cursor::new(data)), None)?;

    assert_eq!(inferred_schema, schema);
    Ok(())
}
