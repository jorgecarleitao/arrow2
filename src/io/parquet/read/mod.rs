//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

mod deserialize;
mod file;
mod row_group;
pub mod schema;
pub mod statistics;

use futures::{AsyncRead, AsyncSeek};

// re-exports of parquet2's relevant APIs
pub use parquet2::{
    error::ParquetError,
    fallible_streaming_iterator,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    page::{CompressedDataPage, DataPage, DataPageHeader},
    read::{
        decompress, get_column_iterator, get_page_iterator as _get_page_iterator,
        get_page_stream as _get_page_stream, read_metadata as _read_metadata,
        read_metadata_async as _read_metadata_async, BasicDecompressor, ColumnChunkIter,
        Decompressor, MutStreamingIterator, PageFilter, PageIterator, ReadColumnIterator, State,
    },
    schema::types::{
        LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType,
        TimeUnit as ParquetTimeUnit, TimestampType,
    },
    types::int96_to_i64_ns,
    FallibleStreamingIterator,
};

pub use deserialize::{column_iter_to_arrays, get_page_iterator};
pub use file::{FileReader, RowGroupReader};
pub use row_group::*;
pub(crate) use schema::is_type_nullable;
pub use schema::{infer_schema, FileMetaData};

use std::{
    io::{Read, Seek},
    sync::Arc,
};

use crate::{array::Array, error::Result};

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
