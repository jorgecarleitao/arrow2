#![cfg(test)]
use arrow2::array::{Array, BinaryArray, BooleanArray, Int32Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::Field;
use arrow2::error::Result;
use arrow2::io::odbc::api::{Connection, Cursor, Environment, Error as OdbcError};
use arrow2::io::odbc::{buffer_from_metadata, deserialize, infer_schema};
use lazy_static::lazy_static;
use stdext::function_name;

lazy_static! {
    /// This is an example for using doc comment attributes
    pub static ref ENV: Environment = Environment::new().unwrap();
}

/// Connection string for our test instance of Microsoft SQL Server
const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;";

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
    let connection = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&connection, table_name, &[type_]).unwrap();
    connection
        .execute(&format!("INSERT INTO {table_name} (a) VALUES {insert}"), ())
        .unwrap();

    // When
    let query = format!("SELECT a FROM {table_name} ORDER BY id");

    let chunks = read(&connection, &query)?.1;

    assert_eq!(chunks, expected);
    Ok(())
}

fn read(
    connection: &Connection<'_>,
    query: &str,
) -> Result<(Vec<Field>, Vec<Chunk<Box<dyn Array>>>)> {
    let mut a = connection.prepare(query).unwrap();
    let fields = infer_schema(&a)?;

    let max_batch_size = 100;
    let buffer = buffer_from_metadata(&a, max_batch_size).unwrap();

    let cursor = a.execute(()).unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();

    let mut chunks = vec![];
    while let Some(batch) = cursor.fetch().unwrap() {
        let arrays = (0..batch.num_cols())
            .zip(fields.iter())
            .map(|(index, field)| {
                let column_view = batch.column(index);
                deserialize(column_view, field.data_type.clone())
            })
            .collect::<Vec<_>>();
        chunks.push(Chunk::new(arrays));
    }

    Ok((fields, chunks))
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection<'_>,
    table_name: &str,
    column_types: &[&str],
) -> std::result::Result<(), OdbcError> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];
    let cols = column_types
        .iter()
        .zip(column_names)
        .map(|(ty, name)| format!("{} {}", name, ty))
        .collect::<Vec<_>>()
        .join(", ");

    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1),{});",
        table_name, cols
    );
    conn.execute(drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
}
