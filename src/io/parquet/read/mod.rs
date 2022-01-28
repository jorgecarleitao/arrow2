//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

use std::{
    collections::VecDeque,
    io::{Read, Seek},
    sync::Arc,
};

use futures::{AsyncRead, AsyncSeek};
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
    array::{Array, BinaryArray, DictionaryKey, NullArray, PrimitiveArray, StructArray, Utf8Array},
    datatypes::{DataType, Field, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
    io::parquet::read::{nested_utils::create_list, primitive::read_item},
};

mod binary;
mod boolean;
mod dictionary;
mod file;
mod fixed_size_binary;
mod nested_utils;
mod primitive;
mod row_group;
pub mod schema;
pub mod statistics;
mod utils;

pub use file::FileReader;
pub use row_group::*;
pub(crate) use schema::is_type_nullable;
pub use schema::{get_schema, FileMetaData};

pub trait DataPages: FallibleStreamingIterator<Item = DataPage, Error = ParquetError> {}
impl<I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError>> DataPages for I {}

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

fn dict_read<'a, K: DictionaryKey, I: 'a + DataPages>(
    iter: I,
    is_optional: bool,
    type_: &ParquetType,
    data_type: DataType,
    chunk_size: usize,
) -> Result<Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>> {
    use DataType::*;
    let values_data_type = if let Dictionary(_, v, _) = &data_type {
        v.as_ref()
    } else {
        panic!()
    };

    Ok(match values_data_type.to_logical_type() {
        UInt8 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: i32| x as u8,
        ),
        UInt16 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: i32| x as u16,
        ),
        UInt32 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: i32| x as u32,
        ),
        Int8 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: i32| x as i8,
        ),
        Int16 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: i32| x as i16,
        ),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                iter,
                is_optional,
                data_type,
                chunk_size,
                |x: i32| x as i32,
            )
        }

        Timestamp(TimeUnit::Nanosecond, None) => match type_ {
            ParquetType::PrimitiveType {
                physical_type,
                logical_type,
                ..
            } => match (physical_type, logical_type) {
                (PhysicalType::Int96, _) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                    iter,
                    is_optional,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    chunk_size,
                    int96_to_i64_ns,
                ),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => match unit {
                    ParquetTimeUnit::MILLIS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        is_optional,
                        data_type,
                        chunk_size,
                        |x: i64| x * 1_000_000,
                    ),
                    ParquetTimeUnit::MICROS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        is_optional,
                        data_type,
                        chunk_size,
                        |x: i64| x * 1_000,
                    ),
                    ParquetTimeUnit::NANOS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        is_optional,
                        data_type,
                        chunk_size,
                        |x: i64| x,
                    ),
                },
                _ => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                    iter,
                    is_optional,
                    data_type,
                    chunk_size,
                    |x: i64| x,
                ),
            },
            _ => unreachable!(),
        },

        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                iter,
                is_optional,
                data_type,
                chunk_size,
                |x: i64| x,
            )
        }
        Float32 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: f32| x,
        ),
        Float64 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            is_optional,
            data_type,
            chunk_size,
            |x: f64| x,
        ),

        Utf8 | Binary => {
            binary::iter_to_dict_arrays::<K, i32, _>(iter, is_optional, data_type, chunk_size)
        }
        LargeUtf8 | LargeBinary => {
            binary::iter_to_dict_arrays::<K, i64, _>(iter, is_optional, data_type, chunk_size)
        }
        other => {
            return Err(ArrowError::nyi(format!(
                "Reading dictionaries of type {:?}",
                other
            )))
        }
    })
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

fn page_iter_to_arrays<'a, I: 'a + DataPages>(
    iter: I,
    metadata: &ColumnChunkMetaData,
    field: Field,
    chunk_size: usize,
) -> Result<Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>> {
    use DataType::*;
    let is_optional =
        metadata.descriptor().max_def_level() != metadata.descriptor().max_rep_level();
    let type_ = metadata.descriptor().type_();
    match field.data_type.to_logical_type() {
        /*Null => Ok(Box::new(NullArray::from_data(
            data_type,
            metadata.num_values() as usize,
        ))),*/
        Boolean => Ok(boolean::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
        )),
        UInt8 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u8,
        )),
        UInt16 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u16,
        )),
        UInt32 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u32,
        )),
        Int8 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as i8,
        )),
        Int16 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as i16,
        )),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            Ok(primitive::iter_to_arrays(
                iter,
                is_optional,
                field.data_type,
                chunk_size,
                read_item,
                |x: i32| x as i32,
            ))
        }

        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType {
                physical_type,
                logical_type,
                ..
            } => match (physical_type, logical_type) {
                (PhysicalType::Int96, _) => Ok(primitive::iter_to_arrays(
                    iter,
                    is_optional,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    chunk_size,
                    read_item,
                    int96_to_i64_ns,
                )),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => Ok(match unit {
                    ParquetTimeUnit::MILLIS(_) => primitive::iter_to_arrays(
                        iter,
                        is_optional,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x * 1_000_000,
                    ),
                    ParquetTimeUnit::MICROS(_) => primitive::iter_to_arrays(
                        iter,
                        is_optional,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x * 1_000,
                    ),
                    ParquetTimeUnit::NANOS(_) => primitive::iter_to_arrays(
                        iter,
                        is_optional,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x,
                    ),
                }),
                _ => Ok(primitive::iter_to_arrays(
                    iter,
                    is_optional,
                    field.data_type,
                    chunk_size,
                    read_item,
                    |x: i64| x,
                )),
            },
            _ => unreachable!(),
        },

        FixedSizeBinary(_) => Ok(Box::new(
            fixed_size_binary::BinaryArrayIterator::new(
                iter,
                field.data_type,
                chunk_size,
                is_optional,
            )
            .map(|x| x.map(|x| Arc::new(x) as _)),
        )),

        Decimal(_, _) => match type_ {
            ParquetType::PrimitiveType { physical_type, .. } => Ok(match physical_type {
                PhysicalType::Int32 => primitive::iter_to_arrays(
                    iter,
                    is_optional,
                    field.data_type,
                    chunk_size,
                    read_item,
                    |x: i32| x as i128,
                ),
                PhysicalType::Int64 => primitive::iter_to_arrays(
                    iter,
                    is_optional,
                    field.data_type,
                    chunk_size,
                    read_item,
                    |x: i64| x as i128,
                ),
                &PhysicalType::FixedLenByteArray(n) if n > 16 => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Can't decode Decimal128 type from Fixed Size Byte Array of len {:?}",
                        n
                    )))
                }
                &PhysicalType::FixedLenByteArray(n) => {
                    let n = n as usize;

                    let iter = fixed_size_binary::BinaryArrayIterator::new(
                        iter,
                        DataType::FixedSizeBinary(n),
                        chunk_size,
                        is_optional,
                    );

                    let iter = iter.map(move |maybe_array| {
                        let array = maybe_array?;
                        let values = array
                            .values()
                            .chunks_exact(n)
                            .map(|value: &[u8]| {
                                // Copy the fixed-size byte value to the start of a 16 byte stack
                                // allocated buffer, then use an arithmetic right shift to fill in
                                // MSBs, which accounts for leading 1's in negative (two's complement)
                                // values.
                                let mut bytes = [0u8; 16];
                                bytes[..n].copy_from_slice(value);
                                i128::from_be_bytes(bytes) >> (8 * (16 - n))
                            })
                            .collect::<Vec<_>>();
                        let validity = array.validity().cloned();

                        Ok(PrimitiveArray::<i128>::from_data(
                            field.data_type.clone(),
                            values.into(),
                            validity,
                        ))
                    });

                    let iter = iter.map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>));

                    Box::new(iter) as _
                }
                _ => unreachable!(),
            }),
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            Ok(primitive::iter_to_arrays(
                iter,
                is_optional,
                field.data_type,
                chunk_size,
                read_item,
                |x: i64| x as i64,
            ))
        }
        UInt64 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: i64| x as u64,
        )),

        Float32 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: f32| x,
        )),
        Float64 => Ok(primitive::iter_to_arrays(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
            read_item,
            |x: f64| x,
        )),

        Binary => Ok(binary::iter_to_arrays::<i32, BinaryArray<i32>, _>(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
        )),
        LargeBinary => Ok(binary::iter_to_arrays::<i64, BinaryArray<i64>, _>(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
        )),
        Utf8 => Ok(binary::iter_to_arrays::<i32, Utf8Array<i32>, _>(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
        )),
        LargeUtf8 => Ok(binary::iter_to_arrays::<i64, Utf8Array<i64>, _>(
            iter,
            is_optional,
            field.data_type,
            chunk_size,
        )),

        Dictionary(key_type, _, _) => match_integer_type!(key_type, |$K| {
            dict_read::<$K, _>(iter, is_optional, type_, field.data_type, chunk_size)
        }),

        List(_) => page_iter_to_arrays_nested(iter, field, chunk_size),
        /*
        LargeList(ref inner) => {
            let values = page_iter_to_array(iter, nested, metadata, inner.data_type().clone());
            create_list(data_type, nested, values.into())
        }
         */
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

fn page_iter_to_arrays_nested<'a, I: 'a + DataPages>(
    iter: I,
    field: Field,
    chunk_size: usize,
) -> Result<Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>> {
    let iter = boolean::iter_to_arrays_nested(iter, field.clone(), chunk_size);

    let iter = iter.map(move |x| {
        let (mut nested, array) = x?;
        let _ = nested.nested.pop().unwrap(); // the primitive
        create_list(field.data_type().clone(), &mut nested, array)
    });

    Ok(Box::new(iter))
}

/*
/// Returns an iterator of [`Array`] built from an iterator of column chunks. It also returns
/// the two buffers used to decompress and deserialize pages (to be re-used).
#[allow(clippy::type_complexity)]
pub fn column_iter_to_arrays<II, I>(
    mut columns: I,
    field: &Field,
    mut buffer: Vec<u8>,
    chunk_size: usize,
) -> Result<(impl Iterator<Item = Box<dyn Array>>, Vec<u8>, Vec<u8>)>
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

                    let array = page_iter_to_arrays(
                        &mut iterator,
                        &mut nested_info,
                        metadata,
                        data_type,
                        chunk_size,
                    )?
                    .collect::<Result<Vec<_>>>()?
                    .pop()
                    .unwrap();
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
 */
