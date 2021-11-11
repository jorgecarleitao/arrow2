//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

use std::{
    convert::TryInto,
    io::{Read, Seek},
    sync::Arc,
};

use futures::{AsyncRead, AsyncSeek, Stream};
pub use parquet2::{
    error::ParquetError,
    fallible_streaming_iterator,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    page::{CompressedDataPage, DataPage, DataPageHeader},
    read::{
        decompress, get_page_iterator as _get_page_iterator, get_page_stream as _get_page_stream,
        read_metadata as _read_metadata, read_metadata_async as _read_metadata_async,
        BasicDecompressor, Decompressor, PageFilter, PageIterator,
    },
    schema::types::{
        LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType,
        TimeUnit as ParquetTimeUnit, TimestampType,
    },
    types::int96_to_i64_ns,
    FallibleStreamingIterator,
};

use crate::{
    array::{Array, DictionaryKey, NullArray, PrimitiveArray},
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
    io::parquet::read::nested_utils::create_list,
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
pub(crate) use schema::is_type_nullable;
pub use schema::{get_schema, FileMetaData};

use self::nested_utils::Nested;

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
    I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError>,
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

    match values_data_type.to_logical_type() {
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
        Utf8 => binary::iter_to_dict_array::<K, i32, _, _>(iter, metadata, data_type),
        LargeUtf8 => binary::iter_to_dict_array::<K, i64, _, _>(iter, metadata, data_type),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading dictionaries of type {:?}",
            other
        ))),
    }
}

fn page_iter_to_array_nested<
    I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError>,
>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<(Arc<dyn Array>, Vec<Box<dyn Nested>>)> {
    use DataType::*;
    match data_type {
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
                _ => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i64| x),
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
        List(ref inner) => {
            let (values, mut nested) =
                page_iter_to_array_nested(iter, metadata, inner.data_type().clone())?;
            Ok((create_list(data_type, &mut nested, values)?.into(), nested))
        }
        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading {:?} from parquet still not implemented",
            other
        ))),
    }
}

/// Converts an iterator of [`DataPage`] into a single [`Array`].
pub fn page_iter_to_array<I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError>>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type.to_logical_type() {
        Null => Ok(Box::new(NullArray::from_data(
            data_type,
            metadata.num_values() as usize,
        ))),
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
        FixedSizeBinary(_) => Ok(Box::new(fixed_size_binary::iter_to_array(
            iter, data_type, metadata,
        )?)),
        Decimal(_, _) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int32 => {
                    primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i128)
                }
                PhysicalType::Int64 => {
                    primitive::iter_to_array(iter, metadata, data_type, |x: i64| x as i128)
                }
                PhysicalType::FixedLenByteArray(n) => {
                    if *n > 16 {
                        Err(ArrowError::NotYetImplemented(format!(
                            "Can't decode Decimal128 type from Fixed Size Byte Array of len {:?}",
                            n
                        )))
                    } else {
                        let paddings = (0..(16 - *n)).map(|_| 0u8).collect::<Vec<_>>();
                        fixed_size_binary::iter_to_array(
                            iter,
                            DataType::FixedSizeBinary(*n as usize),
                            metadata,
                        )
                        .map(|e| {
                            let a = e
                                .into_iter()
                                .map(|v| {
                                    v.and_then(|v1| {
                                        [&paddings, v1]
                                            .concat()
                                            .try_into()
                                            .map(i128::from_be_bytes)
                                            .ok()
                                    })
                                })
                                .collect::<Vec<_>>();
                            Box::new(PrimitiveArray::<i128>::from(a).to(data_type))
                                as Box<dyn Array>
                        })
                    }
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        List(ref inner) => {
            let (values, mut nested) =
                page_iter_to_array_nested(iter, metadata, inner.data_type().clone())?;
            create_list(data_type, &mut nested, values)
        }
        LargeList(ref inner) => {
            let (values, mut nested) =
                page_iter_to_array_nested(iter, metadata, inner.data_type().clone())?;
            create_list(data_type, &mut nested, values)
        }

        Dictionary(key_type, _) => match_integer_type!(key_type, |$T| {
            dict_read::<$T, _>(iter, metadata, data_type)
        }),

        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading {:?} from parquet still not implemented",
            other
        ))),
    }
}

/// Converts an async stream of [`DataPage`] into a single [`Array`].
pub async fn page_stream_to_array<I: Stream<Item = std::result::Result<DataPage, ParquetError>>>(
    pages: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type.to_logical_type() {
        Null => Ok(Box::new(NullArray::from_data(
            data_type,
            metadata.num_values() as usize,
        ))),
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
        FixedSizeBinary(_) => Ok(Box::new(
            fixed_size_binary::stream_to_array(pages, data_type, metadata).await?,
        )),
        other => Err(ArrowError::NotYetImplemented(format!(
            "Async conversion of {:?}",
            other
        ))),
    }
}
