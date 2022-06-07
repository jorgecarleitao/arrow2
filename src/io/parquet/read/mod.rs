//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

mod deserialize;
mod file;
mod indexes;
mod row_group;
pub mod schema;
pub mod statistics;

use std::{
    io::{Read, Seek},
    sync::Arc,
};

use futures::{AsyncRead, AsyncSeek};

// re-exports of parquet2's relevant APIs
pub use parquet2::{
    error::Error as ParquetError,
    fallible_streaming_iterator,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    page::{CompressedDataPage, DataPage, DataPageHeader},
    read::{
        decompress, get_column_iterator, get_page_stream,
        read_columns_indexes as _read_columns_indexes, read_metadata as _read_metadata,
        read_metadata_async as _read_metadata_async, read_pages_locations, BasicDecompressor,
        ColumnChunkIter, Decompressor, MutStreamingIterator, PageFilter, PageReader,
        ReadColumnIterator, State,
    },
    schema::types::{
        GroupLogicalType, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType,
        TimeUnit as ParquetTimeUnit,
    },
    types::int96_to_i64_ns,
    FallibleStreamingIterator,
};

use crate::{array::Array, error::Result};

pub use deserialize::{column_iter_to_arrays, get_page_iterator};
pub use file::{FileReader, RowGroupReader};
pub use indexes::{read_columns_indexes, ColumnIndex};
pub use row_group::*;
pub use schema::{infer_schema, FileMetaData};

/// Trait describing a [`FallibleStreamingIterator`] of [`DataPage`]
pub trait DataPages:
    FallibleStreamingIterator<Item = DataPage, Error = ParquetError> + Send + Sync
{
}

impl<I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError> + Send + Sync> DataPages
    for I
{
}

/// Type def for a sharable, boxed dyn [`Iterator`] of arrays
pub type ArrayIter<'a> = Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + Send + Sync + 'a>;

/// Reads parquets' metadata syncronously.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    Ok(_read_metadata(reader)?)
}

/// Reads parquets' metadata asynchronously.
pub async fn read_metadata_async<R: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut R,
) -> Result<FileMetaData> {
    Ok(_read_metadata_async(reader).await?)
}
