//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

use std::{
    collections::VecDeque,
    convert::TryFrom,
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

use crate::{
    array::{Array, DictionaryKey, NullArray, PrimitiveArray, StructArray},
    bitmap::MutableBitmap,
    datatypes::{DataType, Field, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
    io::parquet::read::{
        fixed_size_binary::extend_from_page,
        nested_utils::{create_list, init_nested},
    },
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
pub fn get_page_iterator<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    reader: R,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
) -> Result<PageIterator<R>> {
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
    let values_data_type = if let Dictionary(_, v, _) = &data_type {
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
            ParquetType::PrimitiveType {
                physical_type,
                logical_type,
                ..
            } => match (physical_type, logical_type) {
                (PhysicalType::Int96, _) => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                    iter,
                    metadata,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    int96_to_i64_ns,
                ),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => match unit {
                    ParquetTimeUnit::MILLIS(_) => {
                        primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                            iter,
                            metadata,
                            data_type,
                            |x: i64| x * 1_000_000,
                        )
                    }
                    ParquetTimeUnit::MICROS(_) => {
                        primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                            iter,
                            metadata,
                            data_type,
                            |x: i64| x * 1_000,
                        )
                    }
                    ParquetTimeUnit::NANOS(_) => primitive::iter_to_dict_array::<K, _, _, _, _, _>(
                        iter,
                        metadata,
                        data_type,
                        |x: i64| x,
                    ),
                },
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

fn column_offset(data_type: &DataType) -> usize {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | FixedSizeBinary | Binary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) | List | LargeList | FixedSizeList => 0,
        Struct => {
            if let DataType::Struct(v) = data_type.to_logical_type() {
                v.iter().map(|x| 1 + column_offset(x.data_type())).sum()
            } else {
                unreachable!()
            }
        }
        Union => todo!(),
        Map => todo!(),
    }
}

fn column_datatype(data_type: &DataType, column: usize) -> DataType {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | FixedSizeBinary | Binary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) | List | LargeList | FixedSizeList => data_type.clone(),
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                let mut total_chunk = 0;
                let mut total_fields = 0;
                for f in fields {
                    let field_chunk = column_offset(f.data_type());
                    if column < total_chunk + field_chunk {
                        return column_datatype(f.data_type(), column + total_chunk);
                    }
                    total_fields += (field_chunk > 0) as usize;
                    total_chunk += field_chunk;
                }
                fields[column + total_fields - total_chunk]
                    .data_type()
                    .clone()
            } else {
                unreachable!()
            }
        }
        Union => todo!(),
        Map => todo!(),
    }
}

fn page_iter_to_array<I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError>>(
    iter: &mut I,
    nested: &mut Vec<Box<dyn Nested>>,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type.to_logical_type() {
        Null => Ok(Box::new(NullArray::from_data(
            data_type,
            metadata.num_values() as usize,
        ))),

        Boolean => boolean::iter_to_array(iter, metadata, data_type, nested),

        UInt8 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as u8),
        UInt16 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as u16),
        UInt32 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as u32),
        Int8 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as i8),
        Int16 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as i16),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as i32)
        }

        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType {
                physical_type,
                logical_type,
                ..
            } => match (physical_type, logical_type) {
                (PhysicalType::Int96, _) => primitive::iter_to_array(
                    iter,
                    metadata,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    nested,
                    int96_to_i64_ns,
                ),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => match unit {
                    ParquetTimeUnit::MILLIS(_) => {
                        primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| {
                            x * 1_000_000
                        })
                    }
                    ParquetTimeUnit::MICROS(_) => {
                        primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| {
                            x * 1_000
                        })
                    }
                    ParquetTimeUnit::NANOS(_) => {
                        primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| x)
                    }
                },
                _ => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| x),
            },
            _ => unreachable!(),
        },

        FixedSizeBinary(_) => Ok(Box::new(fixed_size_binary::iter_to_array(
            iter, data_type, metadata,
        )?)),
        Decimal(_, _) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int32 => {
                    primitive::iter_to_array(iter, metadata, data_type, nested, |x: i32| x as i128)
                }
                PhysicalType::Int64 => {
                    primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| x as i128)
                }
                &PhysicalType::FixedLenByteArray(n) if n > 16 => {
                    Err(ArrowError::NotYetImplemented(format!(
                        "Can't decode Decimal128 type from Fixed Size Byte Array of len {:?}",
                        n
                    )))
                }
                &PhysicalType::FixedLenByteArray(n) => {
                    let n = usize::try_from(n).unwrap();
                    let capacity = metadata.num_values() as usize;
                    let mut validity = MutableBitmap::with_capacity(capacity);
                    let mut byte_values = Vec::<u8>::new();
                    let mut i128_values = Vec::<i128>::with_capacity(capacity);

                    // Iterate through the fixed-size binary pages, converting each fixed-size
                    // value to an i128, and append to `i128_values`. This conversion requires
                    // fully materializing the compressed Parquet page into an uncompressed byte
                    // buffer (`byte_values`), so operating page-at-a-time reduces memory usage as
                    // opposed to operating on the entire chunk.
                    while let Some(page) = iter.next()? {
                        byte_values.clear();
                        byte_values.reserve(page.num_values() * n);

                        extend_from_page(
                            page,
                            n,
                            metadata.descriptor(),
                            &mut byte_values,
                            &mut validity,
                        )?;

                        debug_assert_eq!(byte_values.len() % n, 0);

                        for fixed_size_value in byte_values.as_slice().chunks_exact(n) {
                            // Copy the fixed-size byte value to the start of a 16 byte stack
                            // allocated buffer, then use an arithmetic right shift to fill in
                            // MSBs, which accounts for leading 1's in negative (two's complement)
                            // values.
                            let mut i128_bytes = [0u8; 16];
                            i128_bytes[..n].copy_from_slice(fixed_size_value);
                            i128_values.push(i128::from_be_bytes(i128_bytes) >> (128 - 8 * n));
                        }
                    }

                    Ok(Box::new(PrimitiveArray::<i128>::from_data(
                        data_type,
                        i128_values.into(),
                        Some(validity.into()),
                    )))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| x)
        }
        UInt64 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: i64| x as u64),

        Float32 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: f32| x),
        Float64 => primitive::iter_to_array(iter, metadata, data_type, nested, |x: f64| x),

        Binary | Utf8 => binary::iter_to_array::<i32, _, _>(iter, metadata, data_type, nested),
        LargeBinary | LargeUtf8 => {
            binary::iter_to_array::<i64, _, _>(iter, metadata, data_type, nested)
        }

        Dictionary(key_type, _, _) => match_integer_type!(key_type, |$T| {
            dict_read::<$T, _>(iter, metadata, data_type)
        }),

        List(ref inner) => {
            let values = page_iter_to_array(iter, nested, metadata, inner.data_type().clone())?;
            create_list(data_type, nested, values.into())
        }
        LargeList(ref inner) => {
            let values = page_iter_to_array(iter, nested, metadata, inner.data_type().clone())?;
            create_list(data_type, nested, values.into())
        }

        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading {:?} from parquet still not implemented",
            other
        ))),
    }
}

fn finish_array(data_type: DataType, arrays: &mut VecDeque<Box<dyn Array>>) -> Box<dyn Array> {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | FixedSizeBinary | Binary | LargeBinary | Utf8
        | LargeUtf8 | List | LargeList | FixedSizeList | Dictionary(_) => {
            arrays.pop_front().unwrap()
        }
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                let values = fields
                    .iter()
                    .map(|f| finish_array(f.data_type().clone(), arrays))
                    .map(|x| x.into())
                    .collect();
                Box::new(StructArray::from_data(data_type, values, None))
            } else {
                unreachable!()
            }
        }
        Union => todo!(),
        Map => todo!(),
    }
}

/// Returns an [`Array`] built from an iterator of column chunks. It also returns
/// the two buffers used to decompress and deserialize pages (to be re-used).
#[allow(clippy::type_complexity)]
pub fn column_iter_to_array<II, I>(
    mut columns: I,
    field: &Field,
    mut buffer: Vec<u8>,
) -> Result<(Box<dyn Array>, Vec<u8>, Vec<u8>)>
where
    II: Iterator<Item = std::result::Result<CompressedDataPage, ParquetError>>,
    I: ColumnChunkIter<II>,
{
    let mut nested_info = vec![];
    init_nested(field, 0, &mut nested_info);

    let data_type = field.data_type().clone();

    let mut arrays = VecDeque::new();
    let page_buffer;
    let mut column = 0;
    loop {
        match columns.advance()? {
            State::Some(mut new_iter) => {
                let data_type = column_datatype(&data_type, column);
                if let Some((pages, metadata)) = new_iter.get() {
                    let mut iterator = BasicDecompressor::new(pages, buffer);

                    let array =
                        page_iter_to_array(&mut iterator, &mut nested_info, metadata, data_type)?;
                    buffer = iterator.into_inner();
                    arrays.push_back(array)
                }
                column += 1;
                columns = new_iter;
            }
            State::Finished(b) => {
                page_buffer = b;
                break;
            }
        }
    }

    let array = finish_array(data_type, &mut arrays);
    assert!(arrays.is_empty());
    Ok((array, page_buffer, buffer))
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
