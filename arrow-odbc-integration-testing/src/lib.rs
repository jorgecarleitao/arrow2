#![cfg(test)]
use arrow2::io::odbc::api::{Connection, Environment, Error as OdbcError, Cursor, buffers::{buffer_from_description, BufferDescription, BufferKind, AnyColumnView}};
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
fn test() {
    // Given a table with a single string
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let connection = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&connection, table_name, &["VARCHAR(50)"]).unwrap();
    connection
        .execute(
            &format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!')"),
            (),
        )
        .unwrap();

    // When
    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    let cursor = connection.execute(&query, ()).unwrap().unwrap();
    // This is the maximum number of rows in each batch.
    let max_batch_size = 1;
    // This is the list of C-Types which will be bound to the result columns of the query.
    let buffer_descriptions = vec![BufferDescription {
        kind: BufferKind::Text { max_str_len: 50 },
        nullable: true,
    }];
    let buffer = buffer_from_description(max_batch_size, buffer_descriptions.into_iter());
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let column_buffer = batch.column(0);
    let mut text_it = match column_buffer {
        AnyColumnView::Text(it) => it,
        _ => panic!("Unexpected types")
    };

    // One unwrap, because there is a first element
    // Second unwrap, because we know the value not to be null
    let bytes = text_it.next().unwrap().unwrap();

    let value = std::str::from_utf8(bytes).unwrap();

    assert_eq!(value, "Hello, World!")
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection<'_>,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), OdbcError> {
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
