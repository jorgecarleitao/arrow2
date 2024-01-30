//! APIs to read from ODBC
mod deserialize;
mod schema;

pub use deserialize::deserialize;
pub use schema::infer_schema;

pub use super::api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use super::api::ColumnDescription;
pub use super::api::Error;
pub use super::api::ResultSetMetadata;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::error::Result;
use crate::io::odbc::api::{Connection, ConnectionOptions, Cursor, Environment};

pub struct Reader {
    connection_string: String,
    query: String,
    login_timeout_sec: Option<u32>,
    max_batch_size: Option<usize>,
}

impl Reader {
    pub fn new(
        connection_string: String,
        query: String,
        login_timeout_sec: Option<u32>,
        max_batch_size: Option<usize>,
    ) -> Self {
        Self {
            connection_string,
            query,
            login_timeout_sec,
            max_batch_size,
        }
    }

    pub fn read(&self) -> Result<Vec<Chunk<Box<dyn Array>>>> {
        let env = Environment::new().unwrap();
        let conn: Connection = env
            .connect_with_connection_string(
                self.connection_string.as_str(),
                ConnectionOptions {
                    login_timeout_sec: self.login_timeout_sec,
                },
            )
            .unwrap();

        let mut a = conn.prepare(self.query.as_str()).unwrap();
        let fields = infer_schema(&mut a)?;

        let buffer = buffer_from_metadata(&mut a, self.max_batch_size.unwrap_or(100)).unwrap();

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

        Ok(chunks)
    }
}

/// Creates a [`api::buffers::ColumnarBuffer`] from the metadata.
/// # Errors
/// Iff the driver provides an incorrect [`api::ResultSetMetadata`]
pub fn buffer_from_metadata(
    result_set_metadata: &mut impl ResultSetMetadata,
    capacity: usize,
) -> std::result::Result<ColumnarAnyBuffer, Error> {
    let num_cols: u16 = result_set_metadata.num_result_cols()? as u16;

    let descs = (1..=num_cols).map(|i| {
        let mut col_desc = ColumnDescription::default();
        result_set_metadata.describe_col(i, &mut col_desc).unwrap();
        BufferDesc::from_data_type(col_desc.data_type, col_desc.could_be_nullable()).unwrap()
    });

    Ok(ColumnarAnyBuffer::from_descs(capacity, descs))
}
