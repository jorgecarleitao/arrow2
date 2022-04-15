//! APIs to read from ODBC
mod deserialize;
mod schema;

pub use deserialize::deserialize;
pub use schema::infer_schema;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::Field;
use crate::error::Error;
use api::Cursor;

use super::api;

/// Creates a [`api::buffers::ColumnarBuffer`] from the metadata.
/// # Errors
/// Iff the driver provides an incorrect [`api::ResultSetMetadata`]
pub fn buffer_from_metadata(
    resut_set_metadata: &impl api::ResultSetMetadata,
    max_batch_size: usize,
) -> std::result::Result<api::buffers::ColumnarBuffer<api::buffers::AnyColumnBuffer>, api::Error> {
    let num_cols: u16 = resut_set_metadata.num_result_cols()? as u16;

    let descs = (0..num_cols)
        .map(|index| {
            let mut column_description = api::ColumnDescription::default();

            resut_set_metadata.describe_col(index + 1, &mut column_description)?;

            Ok(api::buffers::BufferDescription {
                nullable: column_description.could_be_nullable(),
                kind: api::buffers::BufferKind::from_data_type(column_description.data_type)
                    .unwrap(),
            })
        })
        .collect::<std::result::Result<Vec<_>, api::Error>>()?;

    Ok(api::buffers::buffer_from_description(
        max_batch_size,
        descs.into_iter(),
    ))
}

type CCursor<'a> = api::RowSetCursor<
    api::CursorImpl<api::handles::StatementImpl<'a>>,
    api::buffers::ColumnarBuffer<api::buffers::AnyColumnBuffer>,
>;

/// An iterator of [`Chunk`]s coming from an
pub struct ChunkIterator<'a> {
    cursor: CCursor<'a>,
    fields: Vec<Field>,
}

impl<'a> ChunkIterator<'a> {
    fn new(cursor: CCursor<'a>, fields: Vec<Field>) -> Self {
        Self { cursor, fields }
    }

    /// The fields as infered from the ODBC fields
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

impl<'a> Iterator for ChunkIterator<'a> {
    type Item = Result<Chunk<Box<dyn Array>>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.fetch().transpose().map(|batch| {
            let batch = batch?;
            let arrays = (0..batch.num_cols())
                .zip(self.fields.iter())
                .map(|(index, field)| {
                    let column_view = batch.column(index);
                    deserialize(column_view, field.data_type.clone())
                })
                .collect::<Vec<_>>();
            Ok(Chunk::new(arrays))
        })
    }
}

/// Returns an iterator of [`Chunk`]
pub fn execute<'a>(
    connection: &api::Connection<'a>,
    query: &str,
    params: impl api::ParameterRefCollection,
    batch_size: Option<usize>,
) -> Result<Option<ChunkIterator<'a>>, Error> {
    let cursor = connection.execute(query, params)?;
    let cursor = if let Some(cursor) = cursor {
        cursor
    } else {
        return Ok(None);
    };
    let batch_size = batch_size.ok_or_else(|| {
        Error::InvalidArgumentError(
            "When execute returns data, the batch_size must be set".to_string(),
        )
    })?;

    let fields = infer_schema(&cursor)?;

    let buffer = buffer_from_metadata(&cursor, batch_size)?;

    let cursor = cursor.bind_buffer(buffer)?;

    Ok(Some(ChunkIterator::new(cursor, fields)))
}
