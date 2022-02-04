//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

use std::{
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
    array::{Array, BinaryArray, DictionaryKey, ListArray, PrimitiveArray, StructArray, Utf8Array},
    datatypes::{DataType, Field, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
    io::parquet::read::primitive::read_item,
};

mod binary;
mod boolean;
mod dictionary;
mod file;
mod fixed_size_binary;
mod nested_utils;
mod null;
mod primitive;
mod row_group;
pub mod schema;
pub mod statistics;
mod utils;

pub use file::{FileReader, RowGroupReader};
pub use row_group::*;
pub(crate) use schema::is_type_nullable;
pub use schema::{get_schema, FileMetaData};

use self::nested_utils::{InitNested, NestedArrayIter, NestedState};

pub trait DataPages:
    FallibleStreamingIterator<Item = DataPage, Error = ParquetError> + Send + Sync
{
}
impl<I: FallibleStreamingIterator<Item = DataPage, Error = ParquetError> + Send + Sync> DataPages
    for I
{
}

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
    type_: &ParquetType,
    data_type: DataType,
    chunk_size: usize,
) -> Result<ArrayIter<'a>> {
    use DataType::*;
    let values_data_type = if let Dictionary(_, v, _) = &data_type {
        v.as_ref()
    } else {
        panic!()
    };

    Ok(match values_data_type.to_logical_type() {
        UInt8 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u8,
        ),
        UInt16 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u16,
        ),
        UInt32 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u32,
        ),
        Int8 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as i8,
        ),
        Int16 => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as i16,
        ),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                iter,
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
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    chunk_size,
                    int96_to_i64_ns,
                ),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => match unit {
                    ParquetTimeUnit::MILLIS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        data_type,
                        chunk_size,
                        |x: i64| x * 1_000_000,
                    ),
                    ParquetTimeUnit::MICROS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        data_type,
                        chunk_size,
                        |x: i64| x * 1_000,
                    ),
                    ParquetTimeUnit::NANOS(_) => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                        iter,
                        data_type,
                        chunk_size,
                        |x: i64| x,
                    ),
                },
                _ => primitive::iter_to_dict_arrays::<K, _, _, _, _>(
                    iter,
                    data_type,
                    chunk_size,
                    |x: i64| x,
                ),
            },
            _ => unreachable!(),
        },

        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(iter, data_type, chunk_size, |x: i64| x)
        }
        Float32 => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(iter, data_type, chunk_size, |x: f32| x)
        }
        Float64 => {
            primitive::iter_to_dict_arrays::<K, _, _, _, _>(iter, data_type, chunk_size, |x: f64| x)
        }

        Utf8 | Binary => binary::iter_to_dict_arrays::<K, i32, _>(iter, data_type, chunk_size),
        LargeUtf8 | LargeBinary => {
            binary::iter_to_dict_arrays::<K, i64, _>(iter, data_type, chunk_size)
        }
        FixedSizeBinary(_) => {
            fixed_size_binary::iter_to_dict_arrays::<K, _>(iter, data_type, chunk_size)
        }
        other => {
            return Err(ArrowError::nyi(format!(
                "Reading dictionaries of type {:?}",
                other
            )))
        }
    })
}

fn page_iter_to_arrays<'a, I: 'a + DataPages>(
    pages: I,
    type_: &ParquetType,
    field: Field,
    chunk_size: usize,
) -> Result<ArrayIter<'a>> {
    use DataType::*;
    match field.data_type.to_logical_type() {
        Null => Ok(null::iter_to_arrays(pages, field.data_type, chunk_size)),
        Boolean => Ok(boolean::iter_to_arrays(pages, field.data_type, chunk_size)),
        UInt8 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u8,
        )),
        UInt16 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u16,
        )),
        UInt32 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as u32,
        )),
        Int8 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as i8,
        )),
        Int16 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i32| x as i16,
        )),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => Ok(
            primitive::iter_to_arrays(pages, field.data_type, chunk_size, read_item, |x: i32| {
                x as i32
            }),
        ),

        Timestamp(TimeUnit::Nanosecond, None) => match type_ {
            ParquetType::PrimitiveType {
                physical_type,
                logical_type,
                ..
            } => match (physical_type, logical_type) {
                (PhysicalType::Int96, _) => Ok(primitive::iter_to_arrays(
                    pages,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    chunk_size,
                    read_item,
                    int96_to_i64_ns,
                )),
                (_, Some(LogicalType::TIMESTAMP(TimestampType { unit, .. }))) => Ok(match unit {
                    ParquetTimeUnit::MILLIS(_) => primitive::iter_to_arrays(
                        pages,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x * 1_000_000,
                    ),
                    ParquetTimeUnit::MICROS(_) => primitive::iter_to_arrays(
                        pages,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x * 1_000,
                    ),
                    ParquetTimeUnit::NANOS(_) => primitive::iter_to_arrays(
                        pages,
                        field.data_type,
                        chunk_size,
                        read_item,
                        |x: i64| x,
                    ),
                }),
                _ => Ok(primitive::iter_to_arrays(
                    pages,
                    field.data_type,
                    chunk_size,
                    read_item,
                    |x: i64| x,
                )),
            },
            _ => unreachable!(),
        },

        FixedSizeBinary(_) => Ok(Box::new(
            fixed_size_binary::BinaryArrayIterator::new(pages, field.data_type, chunk_size)
                .map(|x| x.map(|x| Arc::new(x) as _)),
        )),

        Decimal(_, _) => match type_ {
            ParquetType::PrimitiveType { physical_type, .. } => Ok(match physical_type {
                PhysicalType::Int32 => primitive::iter_to_arrays(
                    pages,
                    field.data_type,
                    chunk_size,
                    read_item,
                    |x: i32| x as i128,
                ),
                PhysicalType::Int64 => primitive::iter_to_arrays(
                    pages,
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

                    let pages = fixed_size_binary::BinaryArrayIterator::new(
                        pages,
                        DataType::FixedSizeBinary(n),
                        chunk_size,
                    );

                    let pages = pages.map(move |maybe_array| {
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

                    let arrays = pages.map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>));

                    Box::new(arrays) as _
                }
                _ => unreachable!(),
            }),
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => Ok(
            primitive::iter_to_arrays(pages, field.data_type, chunk_size, read_item, |x: i64| {
                x as i64
            }),
        ),
        UInt64 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: i64| x as u64,
        )),

        Float32 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: f32| x,
        )),
        Float64 => Ok(primitive::iter_to_arrays(
            pages,
            field.data_type,
            chunk_size,
            read_item,
            |x: f64| x,
        )),

        Binary => Ok(binary::iter_to_arrays::<i32, BinaryArray<i32>, _>(
            pages,
            field.data_type,
            chunk_size,
        )),
        LargeBinary => Ok(binary::iter_to_arrays::<i64, BinaryArray<i64>, _>(
            pages,
            field.data_type,
            chunk_size,
        )),
        Utf8 => Ok(binary::iter_to_arrays::<i32, Utf8Array<i32>, _>(
            pages,
            field.data_type,
            chunk_size,
        )),
        LargeUtf8 => Ok(binary::iter_to_arrays::<i64, Utf8Array<i64>, _>(
            pages,
            field.data_type,
            chunk_size,
        )),

        Dictionary(key_type, _, _) => match_integer_type!(key_type, |$K| {
            dict_read::<$K, _>(pages, type_, field.data_type, chunk_size)
        }),

        /*LargeList(inner) | List(inner) => {
            let data_type = inner.data_type.clone();
            page_iter_to_arrays_nested(pages, type_, field, data_type, chunk_size)
        }*/
        other => Err(ArrowError::NotYetImplemented(format!(
            "Reading {:?} from parquet still not implemented",
            other
        ))),
    }
}

fn create_list(
    data_type: DataType,
    nested: &mut NestedState,
    values: Arc<dyn Array>,
) -> Result<Arc<dyn Array>> {
    Ok(match data_type {
        DataType::List(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
            Arc::new(ListArray::<i32>::from_data(
                data_type,
                offsets.into(),
                values,
                validity,
            ))
        }
        DataType::LargeList(_) => {
            let (offsets, validity) = nested.nested.pop().unwrap().inner();

            Arc::new(ListArray::<i64>::from_data(
                data_type, offsets, values, validity,
            ))
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Read nested datatype {:?}",
                data_type
            )))
        }
    })
}

struct StructIterator<'a> {
    iters: Vec<NestedArrayIter<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    pub fn new(iters: Vec<NestedArrayIter<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Arc<dyn Array>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }
        let values = values
            .into_iter()
            .map(|x| x.unwrap().map(|x| x.1))
            .collect::<Result<Vec<_>>>();

        match values {
            Ok(values) => Some(Ok((
                NestedState::new(vec![]), // todo
                Arc::new(StructArray::from_data(
                    DataType::Struct(self.fields.clone()),
                    values,
                    None,
                )),
            ))),
            Err(e) => Some(Err(e)),
        }
    }
}

fn columns_to_iter_recursive<'a, I: 'a>(
    mut columns: Vec<I>,
    mut types: Vec<&ParquetType>,
    field: Field,
    mut init: Vec<InitNested>,
    chunk_size: usize,
) -> Result<NestedArrayIter<'a>>
where
    I: DataPages,
{
    use DataType::*;
    if init.len() == 1 && init[0].is_primitive() {
        return Ok(Box::new(
            page_iter_to_arrays(
                columns.pop().unwrap(),
                types.pop().unwrap(),
                field,
                chunk_size,
            )?
            .map(|x| Ok((NestedState::new(vec![]), x?))),
        ));
    }

    Ok(match field.data_type().to_logical_type() {
        Boolean => {
            types.pop();
            boolean::iter_to_arrays_nested(columns.pop().unwrap(), init.pop().unwrap(), chunk_size)
        }
        Int16 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                read_item,
                |x: i32| x as i16,
            )
        }
        Int64 => {
            types.pop();
            primitive::iter_to_arrays_nested(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
                read_item,
                |x: i64| x,
            )
        }
        Utf8 => {
            types.pop();
            binary::iter_to_arrays_nested::<i32, Utf8Array<i32>, _>(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
            )
        }
        LargeBinary => {
            types.pop();
            binary::iter_to_arrays_nested::<i64, BinaryArray<i64>, _>(
                columns.pop().unwrap(),
                init.pop().unwrap(),
                field.data_type().clone(),
                chunk_size,
            )
        }
        List(inner) => {
            let iter = columns_to_iter_recursive(
                vec![columns.pop().unwrap()],
                types,
                inner.as_ref().clone(),
                init,
                chunk_size,
            )?;
            let iter = iter.map(move |x| {
                let (mut nested, array) = x?;
                let array = create_list(field.data_type().clone(), &mut nested, array)?;
                Ok((nested, array))
            });
            Box::new(iter) as _
        }
        Struct(fields) => {
            let columns = fields
                .iter()
                .rev()
                .map(|f| {
                    columns_to_iter_recursive(
                        vec![columns.pop().unwrap()],
                        vec![types.pop().unwrap()],
                        f.clone(),
                        vec![init.pop().unwrap()],
                        chunk_size,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let columns = columns.into_iter().rev().collect();
            Box::new(StructIterator::new(columns, fields.clone()))
        }
        _ => todo!(),
    })
}

// [Struct<Int, Utf8>, List<Int>, Bool]
// => [Struct(Int), Struct(Utf8), List(Int), Bool]
// [Struct<Struct<Int>, Utf8>, List<Int>, Bool]
// => [Struct(Struct(Int)), Struct(Utf8), List(Int), Bool]
// [List<Struct<Int, Bool>>]
// => [List(Struct(Int)), List(Struct(Bool))]
// [Struct<Struct<Int, Bool>, Utf8>]
// => [Struct(Int), Struct(Bool)]
// => [Struct(Struct(Int)), Struct(Struct(Bool)), Struct(Utf8)]

fn field_to_init(field: &Field) -> Vec<InitNested> {
    use crate::datatypes::PhysicalType::*;
    match field.data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 => vec![InitNested::Primitive(field.is_nullable)],
        List | FixedSizeList | LargeList => {
            let a = field.data_type().to_logical_type();
            let inner = if let DataType::List(inner) = a {
                field_to_init(inner)
            } else if let DataType::LargeList(inner) = a {
                field_to_init(inner)
            } else if let DataType::FixedSizeList(inner, _) = a {
                field_to_init(inner)
            } else {
                unreachable!()
            };
            inner
                .into_iter()
                .map(|x| InitNested::List(Box::new(x), field.is_nullable))
                .collect()
        }
        Struct => {
            let inner = if let DataType::Struct(fields) = field.data_type.to_logical_type() {
                fields.iter().rev().map(field_to_init).collect::<Vec<_>>()
            } else {
                unreachable!()
            };
            inner
                .into_iter()
                .flatten()
                .map(|x| InitNested::Struct(Box::new(x), field.is_nullable))
                .collect()
        }
        _ => todo!(),
    }
}

/// Returns an iterator of [`Array`] built from an iterator of column chunks.
pub fn column_iter_to_arrays<'a, I: 'static>(
    columns: Vec<I>,
    types: Vec<&ParquetType>,
    field: Field,
    chunk_size: usize,
) -> Result<ArrayIter<'a>>
where
    I: DataPages,
{
    let init = field_to_init(&field);

    Ok(Box::new(
        columns_to_iter_recursive(columns, types, field, init, chunk_size)?.map(|x| x.map(|x| x.1)),
    ))
}

/// Type def for a sharable, boxed dyn [`Iterator`] of arrays
pub type ArrayIter<'a> = Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + Send + Sync + 'a>;
