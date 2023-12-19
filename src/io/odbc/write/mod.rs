//! APIs to write to ODBC
mod schema;
mod serialize;

use crate::{array::Array, chunk::Chunk, error::Result};

use super::api;
use crate::io::odbc::api::{Connection, ConnectionOptions, Environment};
pub use api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use api::ColumnDescription;
pub use schema::data_type_to;
pub use serialize::serialize;

/// Creates a [`api::buffers::ColumnarBuffer`] from [`api::ColumnDescription`]s.
///
/// This is useful when separating the serialization (CPU-bounded) to writing to the DB (IO-bounded).
pub fn buffer_from_description(
    descriptions: Vec<ColumnDescription>,
    capacity: usize,
) -> ColumnarAnyBuffer {
    let descs = descriptions.into_iter().map(|description| {
        BufferDesc::from_data_type(description.data_type, description.could_be_nullable()).unwrap()
    });

    ColumnarAnyBuffer::from_descs(capacity, descs)
}

/// A writer of [`Chunk`]s to an ODBC [`api::Prepared`] statement.
/// # Implementation
/// This struct mixes CPU-bounded and IO-bounded tasks and is not ideal
/// for an `async` context.
pub struct Writer {
    connection_string: String,
    query: String,
    login_timeout_sec: Option<u32>,
}

impl Writer {
    pub fn new(connection_string: String, query: String, login_timeout_sec: Option<u32>) -> Self {
        Self {
            connection_string,
            query,
            login_timeout_sec,
        }
    }

    /// Writes a chunk to the writer.
    /// # Errors
    /// Errors iff the execution of the statement fails.
    pub fn write<A: AsRef<dyn Array>>(&mut self, chunk: &Chunk<A>) -> Result<()> {
        let env = Environment::new().unwrap();

        let conn: Connection = env
            .connect_with_connection_string(
                self.connection_string.as_str(),
                ConnectionOptions {
                    login_timeout_sec: self.login_timeout_sec,
                },
            )
            .unwrap();

        let buf_descs = chunk.arrays().iter().map(|array| {
            BufferDesc::from_data_type(
                data_type_to(array.as_ref().data_type()).unwrap(),
                array.as_ref().null_count() > 0,
            )
            .unwrap()
        });

        let prepared = conn.prepare(self.query.as_str()).unwrap();
        let mut prebound = prepared
            .into_column_inserter(chunk.len(), buf_descs)
            .unwrap();
        prebound.set_num_rows(chunk.len());

        for (i, column) in chunk.arrays().iter().enumerate() {
            serialize(column.as_ref(), &mut prebound.column_mut(i)).unwrap();
        }
        prebound.execute().unwrap();

        Ok(())
    }
}
