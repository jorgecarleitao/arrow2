use std::io::Cursor;
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::{DataType, Field};
use arrow2::error::Result;
use arrow2::io::ndjson::read as ndjson_read;
use arrow2::io::ndjson::read::FallibleStreamingIterator;

fn infer(ndjson: &str) -> Result<DataType> {
    ndjson_read::infer(&mut Cursor::new(ndjson), None)
}

fn read_and_deserialize(
    ndjson: String,
    data_type: DataType,
    batch_size: usize,
) -> Result<Vec<Arc<dyn Array>>> {
    let reader = Cursor::new(ndjson);

    let mut reader = ndjson_read::FileReader::new(reader, vec!["".to_string(); batch_size], None);

    let mut chunks = vec![];
    while let Some(rows) = reader.next()? {
        chunks.push(ndjson_read::deserialize(rows, data_type.clone())?);
    }

    Ok(chunks)
}

#[test]
fn infer_nullable() -> Result<()> {
    let ndjson = r#"true
    false
    null
    true
    "#;
    let expected = DataType::Boolean;

    let result = infer(ndjson)?;

    assert_eq!(result, expected);
    Ok(())
}

fn case_nested_struct() -> (String, Arc<dyn Array>) {
    let ndjson = r#"{"a": {"a": 2.0, "b": 2}}
    {"a": {"b": 2}}
    {"a": {"a": 2.0, "b": 2, "c": true}}
    {"a": {"a": 2.0, "b": 2}}
    "#;

    let inner = DataType::Struct(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Boolean, true),
    ]);

    let data_type = DataType::Struct(vec![Field::new("a", inner.clone(), true)]);

    let values = vec![
        Arc::new(Float64Array::from([Some(2.0), None, Some(2.0), Some(2.0)])) as Arc<dyn Array>,
        Arc::new(Int64Array::from([Some(2), Some(2), Some(2), Some(2)])),
        Arc::new(BooleanArray::from([None, None, Some(true), None])),
    ];

    let values = vec![Arc::new(StructArray::from_data(inner, values, None)) as Arc<dyn Array>];

    let array = Arc::new(StructArray::from_data(data_type, values, None));

    (ndjson.to_string(), array)
}

#[test]
fn infer_nested_struct() -> Result<()> {
    let (ndjson, array) = case_nested_struct();

    let result = infer(&ndjson)?;

    assert_eq!(&result, array.data_type());
    Ok(())
}

#[test]
fn read_nested_struct() -> Result<()> {
    let (ndjson, expected) = case_nested_struct();

    let data_type = infer(&ndjson)?;

    let result = read_and_deserialize(ndjson, data_type, 100)?;

    assert_eq!(result, vec![expected]);
    Ok(())
}

#[test]
fn read_nested_struct_batched() -> Result<()> {
    let (ndjson, expected) = case_nested_struct();
    let batch_size = 2;

    // create a chunked array by batch_size from the (un-chunked) expected
    let expected: Vec<Arc<dyn Array>> = (0..(expected.len() + batch_size - 1) / batch_size)
        .map(|offset| expected.slice(offset * batch_size, batch_size).into())
        .collect();

    let data_type = infer(&ndjson)?;

    let result = read_and_deserialize(ndjson, data_type, batch_size)?;

    assert_eq!(result, expected,);
    Ok(())
}
