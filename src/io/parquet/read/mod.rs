use std::{
    io::{Read, Seek},
    sync::Arc,
};

use futures::{AsyncRead, AsyncSeek, Stream};
pub use parquet2::{
    error::ParquetError,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    page::{CompressedDataPage, DataPage, DataPageHeader},
    read::{
        decompress, get_page_iterator as _get_page_iterator, get_page_stream as _get_page_stream,
        read_metadata as _read_metadata, read_metadata_async as _read_metadata_async,
        streaming_iterator, Decompressor, PageFilter, PageIterator, StreamingIterator,
    },
    schema::types::{
        LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType,
        TimeUnit as ParquetTimeUnit, TimestampType,
    },
    types::int96_to_i64_ns,
};

use crate::{
    array::{Array, DictionaryKey},
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
};

mod binary;
mod boolean;
mod fixed_size_binary;
mod nested_utils;
mod primitive;
mod record_batch;
pub mod schema;
pub mod statistics;
mod utils;

pub use record_batch::RecordReader;
pub use schema::{get_schema, is_type_nullable, FileMetaData};

/// Creates a new iterator of compressed pages.
pub fn get_page_iterator<'b, RR: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    reader: &'b mut RR,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
) -> Result<PageIterator<'b, RR>> {
    Ok(_get_page_iterator(
        column_metadata,
        reader,
        pages_filter,
        buffer,
    )?)
}

/// Creates a new iterator of compressed pages.
pub async fn get_page_stream<'a, RR: AsyncRead + Unpin + Send + AsyncSeek>(
    column_metadata: &'a ColumnChunkMetaData,
    reader: &'a mut RR,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
) -> Result<impl Stream<Item = std::result::Result<CompressedDataPage, ParquetError>> + 'a> {
    let pages_filter = pages_filter.unwrap_or_else(|| Arc::new(|_, _| true));
    Ok(_get_page_stream(column_metadata, reader, buffer, pages_filter).await?)
}

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

fn dict_read<
    K: DictionaryKey,
    I: StreamingIterator<Item = std::result::Result<DataPage, ParquetError>>,
>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    let values_data_type = if let Dictionary(_, v) = &data_type {
        v.as_ref()
    } else {
        panic!()
    };

    match values_data_type {
        UInt8 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
            iter,
            metadata,
            data_type,
            |x: i32| x as u8,
        ),
        UInt16 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
            iter,
            metadata,
            data_type,
            |x: i32| x as u16,
        ),
        UInt32 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
            iter,
            metadata,
            data_type,
            |x: i32| x as u32,
        ),
        Int8 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
            iter,
            metadata,
            data_type,
            |x: i32| x as i8,
        ),
        Int16 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
            iter,
            metadata,
            data_type,
            |x: i32| x as i16,
        ),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                iter,
                metadata,
                data_type,
                |x: i32| x as i32,
            )
        }
        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int96 => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                    iter,
                    metadata,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    int96_to_i64_ns,
                ),
                _ => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                    iter,
                    metadata,
                    data_type,
                    |x: i64| x,
                ),
            },
            _ => unreachable!(),
        },
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_dict_array::<K, _, _, _, _, _>(iter, metadata, data_type, |x: i64| x)
        }
        Utf8 => binary::iter_to_dict_array::<K, i32, _, _>(iter, metadata),
        LargeUtf8 => binary::iter_to_dict_array::<K, i64, _, _>(iter, metadata),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading dictionaries of type {:?}",
            other
        ))),
    }
}

pub fn page_iter_to_array<
    I: StreamingIterator<Item = std::result::Result<DataPage, ParquetError>>,
>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type {
        // INT32
        UInt8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u8),
        UInt16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u16),
        UInt32 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u32),
        Int8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i8),
        Int16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i16),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i32)
        }

        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int96 => primitive::iter_to_array(
                    iter,
                    metadata,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    int96_to_i64_ns,
                ),
                _ => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x),
            },
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_array(iter, metadata, data_type, |x: i64| x)
        }
        UInt64 => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x as u64),

        Float32 => primitive::iter_to_array(iter, metadata, data_type, |x: f32| x),
        Float64 => primitive::iter_to_array(iter, metadata, data_type, |x: f64| x),

        Boolean => Ok(Box::new(boolean::iter_to_array(iter, metadata)?)),

        Binary | Utf8 => binary::iter_to_array::<i32, _, _>(iter, metadata, &data_type),
        LargeBinary | LargeUtf8 => binary::iter_to_array::<i64, _, _>(iter, metadata, &data_type),
        FixedSizeBinary(size) => Ok(Box::new(fixed_size_binary::iter_to_array(
            iter, size, metadata,
        )?)),

        List(ref inner) => match inner.data_type() {
            UInt8 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u8),
            UInt16 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u16),
            UInt32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u32),
            Int8 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i8),
            Int16 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i16),
            Int32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i32),

            Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
                ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                    PhysicalType::Int96 => primitive::iter_to_array_nested(
                        iter,
                        metadata,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        int96_to_i64_ns,
                    ),
                    _ => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x),
                },
                _ => unreachable!(),
            },

            // INT64
            Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
                primitive::iter_to_array_nested(iter, metadata, data_type, |x: i64| x)
            }
            UInt64 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i64| x as u64),

            Float32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: f32| x),
            Float64 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: f64| x),

            Boolean => boolean::iter_to_array_nested(iter, metadata, data_type),

            Binary | Utf8 => binary::iter_to_array_nested::<i32, _, _>(iter, metadata, data_type),
            LargeBinary | LargeUtf8 => {
                binary::iter_to_array_nested::<i64, _, _>(iter, metadata, data_type)
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "The conversion of {:?} to arrow still not implemented",
                other
            ))),
        },

        Dictionary(ref key, _) => match key.as_ref() {
            Int8 => dict_read::<i8, _>(iter, metadata, data_type),
            Int16 => dict_read::<i16, _>(iter, metadata, data_type),
            Int32 => dict_read::<i32, _>(iter, metadata, data_type),
            Int64 => dict_read::<i64, _>(iter, metadata, data_type),
            UInt8 => dict_read::<u8, _>(iter, metadata, data_type),
            UInt16 => dict_read::<u16, _>(iter, metadata, data_type),
            UInt32 => dict_read::<u32, _>(iter, metadata, data_type),
            UInt64 => dict_read::<u64, _>(iter, metadata, data_type),
            _ => unreachable!(),
        },

        other => Err(ArrowError::NotYetImplemented(format!(
            "The conversion of {:?} to arrow still not implemented",
            other
        ))),
    }
}

// Converts an async stream of compressed data pages into an [`Array`].
pub async fn page_stream_to_array<I: Stream<Item = std::result::Result<DataPage, ParquetError>>>(
    pages: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type {
        // INT32
        UInt8 => primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as u8).await,
        UInt16 => primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as u16).await,
        UInt32 => primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as u32).await,
        Int8 => primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as i8).await,
        Int16 => primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as i16).await,
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::stream_to_array(pages, metadata, data_type, |x: i32| x as i32).await
        }

        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int96 => {
                    primitive::stream_to_array(
                        pages,
                        metadata,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        int96_to_i64_ns,
                    )
                    .await
                }
                _ => primitive::stream_to_array(pages, metadata, data_type, |x: i64| x).await,
            },
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::stream_to_array(pages, metadata, data_type, |x: i64| x).await
        }
        UInt64 => primitive::stream_to_array(pages, metadata, data_type, |x: i64| x as u64).await,

        Float32 => primitive::stream_to_array(pages, metadata, data_type, |x: f32| x).await,
        Float64 => primitive::stream_to_array(pages, metadata, data_type, |x: f64| x).await,

        Boolean => Ok(Box::new(boolean::stream_to_array(pages, metadata).await?)),

        Binary | Utf8 => binary::stream_to_array::<i32, _, _>(pages, metadata, &data_type).await,
        LargeBinary | LargeUtf8 => {
            binary::stream_to_array::<i64, _, _>(pages, metadata, &data_type).await
        }
        FixedSizeBinary(size) => Ok(Box::new(
            fixed_size_binary::stream_to_array(pages, size, metadata).await?,
        )),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Async conversion of {:?}",
            other
        ))),
    }
}
