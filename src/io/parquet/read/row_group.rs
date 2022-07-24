use std::io::{Read, Seek};

use futures::{
    future::{try_join_all, BoxFuture},
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt,
};
use parquet2::{
    metadata::ColumnChunkMetaData,
    read::{BasicDecompressor, PageReader},
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
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            return None;
        }
        let chunk = self
            .column_chunks
            .iter_mut()
            .map(|iter| iter.next().unwrap())
            .collect::<Result<Vec<_>>>()
            .and_then(Chunk::try_new);
        self.remaining_rows = self.remaining_rows.saturating_sub(
            chunk
                .as_ref()
                .map(|x| x.len())
                .unwrap_or(self.remaining_rows),
        );

        Some(chunk)
    }
}

/// Returns all [`ColumnChunkMetaData`] associated to `field_name`.
/// For non-nested parquet types, this returns a single column
pub fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Vec<&'a ColumnChunkMetaData> {
    columns
        .iter()
        .filter(|x| x.descriptor().path_in_schema[0] == field_name)
        .collect()
}

/// Reads all columns that are part of the parquet field `field_name`
/// # Implementation
/// This operation is IO-bounded `O(C)` where C is the number of columns associated to
/// the field (one for non-nested types)
pub fn read_columns<'a, R: Read + Seek>(
    reader: &mut R,
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    get_field_columns(columns, field_name)
        .into_iter()
        .map(|meta| _read_single_column(reader, meta))
        .collect()
}

fn _read_single_column<'a, R>(
    reader: &mut R,
    meta: &'a ColumnChunkMetaData,
) -> Result<(&'a ColumnChunkMetaData, Vec<u8>)>
where
    R: Read + Seek,
{
    let (start, length) = meta.byte_range();
    reader.seek(std::io::SeekFrom::Start(start))?;

    let mut chunk = vec![];
    chunk.try_reserve(length as usize)?;
    reader
        .by_ref()
        .take(length as u64)
        .read_to_end(&mut chunk)?;
    Ok((meta, chunk))
}

async fn _read_single_column_async<'b, R, F>(
    factory: F,
    meta: &ColumnChunkMetaData,
) -> Result<(&ColumnChunkMetaData, Vec<u8>)>
where
    R: AsyncRead + AsyncSeek + Send + Unpin,
    F: Fn() -> BoxFuture<'b, std::io::Result<R>>,
{
    let mut reader = factory().await?;
    let (start, length) = meta.byte_range();
    reader.seek(std::io::SeekFrom::Start(start)).await?;

    let mut chunk = vec![];
    chunk.try_reserve(length as usize)?;
    reader.take(length as u64).read_to_end(&mut chunk).await?;
    Result::Ok((meta, chunk))
}

/// Reads all columns that are part of the parquet field `field_name`
/// # Implementation
/// This operation is IO-bounded `O(C)` where C is the number of columns associated to
/// the field (one for non-nested types)
///
/// It does so asynchronously via a single `join_all` over all the necessary columns for
/// `field_name`.
pub async fn read_columns_async<
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
        .map(|meta| async { _read_single_column_async(factory.clone(), meta).await });

    try_join_all(futures).await
}

/// Converts a vector of columns associated with the parquet field whose name is [`Field`]
/// to an iterator of [`Array`], [`ArrayIter`] of chunk size `chunk_size`.
pub fn to_deserializer<'a>(
    columns: Vec<(&ColumnChunkMetaData, Vec<u8>)>,
    field: Field,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<ArrayIter<'a>> {
    let chunk_size = chunk_size.unwrap_or(usize::MAX).min(num_rows);

    let (columns, types): (Vec<_>, Vec<_>) = columns
        .into_iter()
        .map(|(column_meta, chunk)| {
            let pages = PageReader::new(
                std::io::Cursor::new(chunk),
                column_meta,
                std::sync::Arc::new(|_, _| true),
                vec![],
            );
            (
                BasicDecompressor::new(pages, vec![]),
                &column_meta.descriptor().descriptor.primitive_type,
            )
        })
        .unzip();

    column_iter_to_arrays(columns, types, field, Some(chunk_size))
}

/// Returns a vector of iterators of [`Array`] ([`ArrayIter`]) corresponding to the top
/// level parquet fields whose name matches `fields`'s names.
///
/// # Implementation
/// This operation is IO-bounded `O(C)` where C is the number of columns in the row group -
/// it reads all the columns to memory from the row group associated to the requested fields.
///
/// This operation is single-threaded. For readers with stronger invariants
/// (e.g. implement [`Clone`]) you can use [`read_columns`] to read multiple columns at once
/// and convert them to [`ArrayIter`] via [`to_deserializer`].
pub fn read_columns_many<'a, R: Read + Seek>(
    reader: &mut R,
    row_group: &RowGroupMetaData,
    fields: Vec<Field>,
    chunk_size: Option<usize>,
    limit: Option<usize>,
) -> Result<Vec<ArrayIter<'a>>> {
    let num_rows = row_group.num_rows();
    let num_rows = limit.map(|limit| limit.min(num_rows)).unwrap_or(num_rows);

    // reads all the necessary columns for all fields from the row group
    // This operation is IO-bounded `O(C)` where C is the number of columns in the row group
    let field_columns = fields
        .iter()
        .map(|field| read_columns(reader, row_group.columns(), &field.name))
        .collect::<Result<Vec<_>>>()?;

    field_columns
        .into_iter()
        .zip(fields.into_iter())
        .map(|(columns, field)| to_deserializer(columns, field, num_rows, chunk_size))
        .collect()
}

/// Returns a vector of iterators of [`Array`] corresponding to the top level parquet fields whose
/// name matches `fields`'s names.
///
/// This operation is IO-bounded `O(C)` where C is the number of columns in the row group -
/// it reads all the columns to memory from the row group associated to the requested fields.
///
/// # Implementation
/// This operation is IO-bounded `O(C)` where C is the number of columns in the row group -
/// it reads all the columns to memory from the row group associated to the requested fields.
/// It does so asynchronously via `join_all`
pub async fn read_columns_many_async<
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
        .map(|field| read_columns_async(factory.clone(), row_group.columns(), &field.name));

    let field_columns = try_join_all(futures).await?;

    field_columns
        .into_iter()
        .zip(fields.into_iter())
        .map(|(columns, field)| {
            to_deserializer(columns, field, row_group.num_rows() as usize, chunk_size)
        })
        .collect()
}
