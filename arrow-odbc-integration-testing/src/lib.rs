#![cfg(test)]
use arrow2::array::Int32Array;
use arrow2::chunk::Chunk;
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
fn test() -> Result<()> {
    // Given a table with a single string
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let connection = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&connection, table_name, &["INT"]).unwrap();
    connection
        .execute(&format!("INSERT INTO {table_name} (a) VALUES (1)"), ())
        .unwrap();

    // When
    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    let mut a = connection.prepare(&query).unwrap();
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

    assert_eq!(
        chunks,
        vec![Chunk::new(vec![Box::new(Int32Array::from_slice([1])) as _])]
    );

    Ok(())
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
