//! APIs to write to ODBC
mod schema;
mod serialize;

use super::api;
pub use schema::infer_descriptions;
pub use serialize::serialize;

/// Creates a [`api::buffers::ColumnarBuffer`] from [`api::ColumnDescription`]s.
pub fn buffer_from_description(
    descriptions: Vec<api::ColumnDescription>,
    max_batch_size: usize,
) -> api::buffers::ColumnarBuffer<api::buffers::AnyColumnBuffer> {
    let descs = descriptions
        .into_iter()
        .map(|description| api::buffers::BufferDescription {
            nullable: description.could_be_nullable(),
            kind: api::buffers::BufferKind::from_data_type(description.data_type).unwrap(),
        });

    let mut buffer = api::buffers::buffer_from_description(max_batch_size, descs);
    buffer.set_num_rows(max_batch_size);
    buffer
}
