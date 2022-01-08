use std::io::Cursor;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::json::read;

use super::*;

fn test_case(case_: &str) -> Result<()> {
    let (data, fields, columns) = case(case_);

    let batch = read_batch(data, &fields)?;

    columns
        .iter()
        .zip(batch.columns())
        .for_each(|(expected, result)| assert_eq!(expected.as_ref(), result.as_ref()));
    Ok(())
}

#[test]
fn basic() -> Result<()> {
    test_case("basics")
}

#[test]
fn projection() -> Result<()> {
    test_case("projection")
}

#[test]
fn dictionary() -> Result<()> {
    test_case("dict")
}

#[test]
fn list() -> Result<()> {
    test_case("list")
}

#[test]
fn nested_struct() -> Result<()> {
    test_case("struct")
}

#[test]
fn nested_list() -> Result<()> {
    test_case("nested_list")
}

#[test]
fn line_break_in_values() -> Result<()> {
    let data = r#"
    {"a":"aa\n\n"}
    {"a":"aa\n"}
    {"a":null}
    "#;

    let batch = read_batch(data.to_string(), &[Field::new("a", DataType::Utf8, true)])?;

    let expected = Utf8Array::<i32>::from(&[Some("aa\n\n"), Some("aa\n"), None]);

    assert_eq!(expected, batch.columns()[0].as_ref());
    Ok(())
}

#[test]
fn invalid_infer_schema() -> Result<()> {
    let re = read::infer(&mut Cursor::new("city,lat,lng"), None);
    assert_eq!(
        re.err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
    Ok(())
}

#[test]
fn invalid_read_record() -> Result<()> {
    let fields = vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
        true,
    )];
    let batch = read_batch("city,lat,lng".to_string(), &fields);

    assert_eq!(
        batch.err().unwrap().to_string(),
        "External error: expected value at line 1 column 1",
    );
    Ok(())
}

#[test]
fn skip_empty_lines() {
    let data = "
    {\"a\": 1}

    {\"a\": 2}

    {\"a\": 3}";

    let batch = read_batch(data.to_string(), &[Field::new("a", DataType::Int64, true)]).unwrap();

    assert_eq!(1, batch.arrays().len());
    assert_eq!(3, batch.len());
}

#[test]
fn row_type_validation() {
    let data = "
    [1, \"hello\"]
    \"world\"";

    let batch = read::infer(&mut Cursor::new(data.to_string()), None);
    assert_eq!(
        batch.err().unwrap().to_string(),
        r#"External format error: Expected JSON record to be an object, found Array([Number(1), String("hello")])"#,
    );
}

#[test]
fn infer_schema_mixed_list() -> Result<()> {
    let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
    {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
    {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
    {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
    "#;

    let fields = vec![
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
    ];

    let result = read::infer(&mut Cursor::new(data), None)?;

    assert_eq!(result, fields);
    Ok(())
}
