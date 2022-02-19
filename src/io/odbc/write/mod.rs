//! APIs to write to ODBC
mod schema;
mod serialize;

use crate::{array::Array, chunk::Chunk, datatypes::Field, error::Result};

use super::api;
pub use schema::infer_descriptions;
pub use serialize::serialize;

/// Creates a [`api::buffers::ColumnarBuffer`] from [`api::ColumnDescription`]s.
pub fn buffer_from_description(
    descriptions: Vec<api::ColumnDescription>,
    capacity: usize,
) -> api::buffers::ColumnarBuffer<api::buffers::AnyColumnBuffer> {
    let descs = descriptions
        .into_iter()
        .map(|description| api::buffers::BufferDescription {
            nullable: description.could_be_nullable(),
            kind: api::buffers::BufferKind::from_data_type(description.data_type).unwrap(),
        });

    api::buffers::buffer_from_description(capacity, descs)
}

/// A writer of [`Chunk`] to an ODBC prepared statement.
pub struct Writer<'a> {
    fields: Vec<Field>,
    buffer: api::buffers::ColumnarBuffer<api::buffers::AnyColumnBuffer>,
    prepared: api::Prepared<'a>,
}

impl<'a> Writer<'a> {
    /// Creates a new [`Writer`]
    pub fn try_new(prepared: api::Prepared<'a>, fields: Vec<Field>) -> Result<Self> {
        let buffer = buffer_from_description(infer_descriptions(&fields)?, 0);
        Ok(Self {
            fields,
            buffer,
            prepared,
        })
    }

    /// Writes a chunk to the writter.
    pub fn write<A: AsRef<dyn Array>>(&mut self, chunk: &Chunk<A>) -> Result<()> {
        if chunk.len() > self.buffer.num_rows() {
            // if the chunk is larger, we re-allocate new buffers to hold it
            self.buffer = buffer_from_description(infer_descriptions(&self.fields)?, chunk.len());
        }

        self.buffer.set_num_rows(chunk.len());

        // serialize (CPU-bounded)
        for (i, column) in chunk.arrays().iter().enumerate() {
            serialize(column.as_ref(), &mut self.buffer.column_mut(i))?;
        }

        // write (IO-bounded)
        self.prepared.execute(&self.buffer)?;
        Ok(())
    }
}
