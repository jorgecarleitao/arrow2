use stdext::function_name;

use arrow2::array::{Array, BinaryArray, BooleanArray, Int32Array, Int64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, TimeUnit};
use arrow2::error::Result;
use arrow2::io::odbc::api::ConnectionOptions;
use arrow2::io::odbc::read::Reader;

use super::{setup_empty_table, ENV, MSSQL};

#[test]
fn int() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![Box::new(Int32Array::from_slice([1])) as _])];

    test(expected, "INT", "(1)", table_name)
}

#[test]
fn int_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![
        Box::new(Int32Array::from([Some(1), None])) as _,
    ])];

    test(expected, "INT", "(1),(NULL)", table_name)
}

#[test]
fn bool() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![
        Box::new(BooleanArray::from_slice([true])) as _
    ])];

    test(expected, "BIT", "(1)", table_name)
}

#[test]
fn bool_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![
        Box::new(BooleanArray::from([Some(true), None])) as _,
    ])];

    test(expected, "BIT", "(1),(NULL)", table_name)
}

#[test]
fn date_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected =
        vec![Chunk::new(vec![
            Box::new(Int32Array::from([Some(100), None]).to(DataType::Date32)) as _,
        ])];

    test(expected, "DATE", "('1970-04-11'),(NULL)", table_name)
}

#[test]
fn timestamp_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![Box::new(
        Int64Array::from([Some(60 * 60 * 1000), None])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
    ) as _])];

    test(
        expected,
        "DATETIME",
        "('1970-01-01 01:00:00'),(NULL)",
        table_name,
    )
}

#[test]
fn timestamp_ms_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![Box::new(
        Int64Array::from([Some(60 * 60 * 1000 + 110), None])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
    ) as _])];

    test(
        expected,
        "DATETIME",
        "('1970-01-01 01:00:00.110'),(NULL)",
        table_name,
    )
}

#[test]
fn binary() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected = vec![Chunk::new(vec![
        Box::new(BinaryArray::<i32>::from([Some(b"ab")])) as _,
    ])];

    test(
        expected,
        "VARBINARY(2)",
        "(CAST('ab' AS VARBINARY(2)))",
        table_name,
    )
}

#[test]
fn binary_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected =
        vec![Chunk::new(vec![
            Box::new(BinaryArray::<i32>::from([Some(b"ab"), None, Some(b"ac")])) as _,
        ])];

    test(
        expected,
        "VARBINARY(2)",
        "(CAST('ab' AS VARBINARY(2))),(NULL),(CAST('ac' AS VARBINARY(2)))",
        table_name,
    )
}

#[test]
fn utf8_nullable() -> Result<()> {
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let expected =
        vec![Chunk::new(vec![
            Box::new(Utf8Array::<i32>::from([Some("ab"), None, Some("ac")])) as _,
        ])];

    test(expected, "VARCHAR(2)", "('ab'),(NULL),('ac')", table_name)
}

fn test(
    expected: Vec<Chunk<Box<dyn Array>>>,
    type_: &str,
    insert: &str,
    table_name: &str,
) -> Result<()> {
    let connection = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table(&connection, table_name, &[type_]).unwrap();

    connection
        .execute(&format!("INSERT INTO {table_name} (a) VALUES {insert}"), ())
        .unwrap();

    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    let chunks = Reader::new(MSSQL.to_string(), query, None, None).read()?;

    assert_eq!(chunks, expected);
    Ok(())
}
