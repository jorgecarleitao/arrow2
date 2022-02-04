use std::{
    io::{Read, Seek},
    sync::Arc,
};

use futures::{
    future::{try_join_all, BoxFuture},
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt,
};
use parquet2::{
    metadata::ColumnChunkMetaData,
    read::{BasicDecompressor, PageIterator},
};

use crate::{
    array::Array, chunk::Chunk, datatypes::Field, error::Result,
    io::parquet::read::column_iter_to_arrays,
};

use super::ArrayIter;
use super::RowGroupMetaData;

/// An [`Iterator`] of [`Chunk`] that (dynamically) adapts a vector of iterators of [`Array`] into
/// an iterator of [`Chunk`].
///
/// This struct tracks advances each of the iterators individually and combines the
/// result in a single [`Chunk`].
///
/// # Implementation
/// This iterator is single-threaded and advancing it is CPU-bounded.
pub struct RowGroupDeserializer {
    num_rows: usize,
    remaining_rows: usize,
    column_chunks: Vec<ArrayIter<'static>>,
}

impl RowGroupDeserializer {
    /// Creates a new [`RowGroupDeserializer`].
    ///
    /// # Panic
    /// This function panics iff any of the `column_chunks`
    /// do not return an array with an equal length.
    pub fn new(
        column_chunks: Vec<ArrayIter<'static>>,
        num_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        Self {
            num_rows,
            remaining_rows: limit.unwrap_or(usize::MAX).min(num_rows),
            column_chunks,
        }
    }

    /// Returns the number of rows on this row group
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

impl Iterator for RowGroupDeserializer {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            return None;
        }
        let chunk = self
            .column_chunks
            .iter_mut()
            .map(|iter| {
                let array = iter.next().unwrap()?;
                Ok(if array.len() > self.remaining_rows {
                    array.slice(0, array.len() - self.remaining_rows).into()
                } else {
                    array
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(Chunk::new);
        self.remaining_rows -= chunk
            .as_ref()
            .map(|x| x.len())
            .unwrap_or(self.remaining_rows);

        Some(chunk)
    }
}

/// Returns all the column metadata in `row_group` associated to `field_name`.
fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Vec<&'a ColumnChunkMetaData> {
    columns
        .iter()
        .enumerate()
        .filter(|x| x.1.descriptor().path_in_schema()[0] == field_name)
        .map(|x| x.1)
        .collect()
}

/// Reads all columns that are part of the parquet field `field_name`
pub(super) fn _read_columns<'a, R: Read + Seek>(
    reader: &mut R,
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    get_field_columns(columns, field_name)
        .into_iter()
        .map(|meta| {
            let (start, len) = meta.byte_range();
            reader.seek(std::io::SeekFrom::Start(start))?;
            let mut chunk = vec![0; len as usize];
            reader.read_exact(&mut chunk)?;
            Ok((meta, chunk))
        })
        .collect()
}

async fn _read_single_column<'b, R, F>(
    factory: F,
    meta: &ColumnChunkMetaData,
) -> Result<(&ColumnChunkMetaData, Vec<u8>)>
where
    R: AsyncRead + AsyncSeek + Send + Unpin,
    F: Fn() -> BoxFuture<'b, std::io::Result<R>>,
{
    let mut reader = factory().await?;
    let (start, len) = meta.byte_range();
    reader.seek(std::io::SeekFrom::Start(start)).await?;
    let mut chunk = vec![0; len as usize];
    reader.read_exact(&mut chunk).await?;
    Result::Ok((meta, chunk))
}

/// Reads all columns that are part of the parquet field `field_name`
async fn _read_columns_async<
    'a,
    'b,
    R: AsyncRead + AsyncSeek + Send + Unpin,
    F: Fn() -> BoxFuture<'b, std::io::Result<R>> + Clone,
>(
    factory: F,
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    let futures = get_field_columns(columns, field_name)
        .into_iter()
        .map(|meta| async { _read_single_column(factory.clone(), meta).await });

    try_join_all(futures).await
}

fn to_deserializers<'a>(
    field_columns: Vec<Vec<(&ColumnChunkMetaData, Vec<u8>)>>,
    fields: Vec<Field>,
    row_group: &RowGroupMetaData,
    chunk_size: Option<usize>,
) -> Result<Vec<ArrayIter<'a>>> {
    let chunk_size = chunk_size
        .unwrap_or(usize::MAX)
        .min(row_group.num_rows() as usize);

    field_columns
        .into_iter()
        .map(|columns| {
            let (columns, types): (Vec<_>, Vec<_>) = columns
                .into_iter()
                .map(|(column_meta, chunk)| {
                    let pages = PageIterator::new(
                        std::io::Cursor::new(chunk),
                        column_meta.num_values(),
                        column_meta.compression(),
                        column_meta.descriptor().clone(),
                        Arc::new(|_, _| true),
                        vec![],
                    );
                    (
                        BasicDecompressor::new(pages, vec![]),
                        column_meta.descriptor().type_(),
                    )
                })
                .unzip();
            (columns, types)
        })
        .zip(fields.into_iter())
        .map(|((columns, types), field)| column_iter_to_arrays(columns, types, field, chunk_size))
        .collect()
}

/// Returns a vector of iterators of [`Array`] corresponding to the top level parquet fields whose
/// name matches `fields`'s names.
///
/// This operation is IO-bounded `O(C)` where C is the number of columns in the row group -
/// it reads all the columns to memory from the row group associated to the requested fields.
pub fn read_columns<'a, R: Read + Seek>(
    reader: &mut R,
    row_group: &RowGroupMetaData,
    fields: Vec<Field>,
    chunk_size: Option<usize>,
) -> Result<Vec<ArrayIter<'a>>> {
    // reads all the necessary columns for all fields from the row group
    // This operation is IO-bounded `O(C)` where C is the number of columns in the row group
    let field_columns = fields
        .iter()
        .map(|field| _read_columns(reader, row_group.columns(), &field.name))
        .collect::<Result<Vec<_>>>()?;

    to_deserializers(field_columns, fields, row_group, chunk_size)
}

/// Returns a vector of iterators of [`Array`] corresponding to the top level parquet fields whose
/// name matches `fields`'s names.
///
/// This operation is IO-bounded `O(C)` where C is the number of columns in the row group -
/// it reads all the columns to memory from the row group associated to the requested fields.
///
/// # Implementation
/// This function concurrently reads all columns.
pub async fn read_columns_async<
    'a,
    'b,
    R: AsyncRead + AsyncSeek + Send + Unpin,
    F: Fn() -> BoxFuture<'b, std::io::Result<R>> + Clone,
>(
    factory: F,
    row_group: &RowGroupMetaData,
    fields: Vec<Field>,
    chunk_size: Option<usize>,
) -> Result<Vec<ArrayIter<'a>>> {
    let futures = fields
        .iter()
        .map(|field| _read_columns_async(factory.clone(), row_group.columns(), &field.name));

    let field_columns = try_join_all(futures).await?;

    to_deserializers(field_columns, fields, row_group, chunk_size)
}
