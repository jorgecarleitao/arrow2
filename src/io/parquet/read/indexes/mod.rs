use parquet2::indexes::{
    BooleanIndex, ByteIndex, FixedLenByteIndex, Index as ParquetIndex, NativeIndex,
};
use parquet2::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet2::read::read_columns_indexes as _read_columns_indexes;
use parquet2::schema::types::PhysicalType as ParquetPhysicalType;

mod binary;
mod boolean;
mod fixed_len_binary;
mod primitive;

use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::{DictionaryArray, ListArray, PrimitiveArray, StructArray};
use crate::datatypes::{Field, PrimitiveType};
use crate::{
    array::Array,
    datatypes::{DataType, PhysicalType},
    error::Error,
};

use super::get_field_pages;

/// Arrow-deserialized [`ColumnIndex`] containing the minimum and maximum value
/// of every page from the column.
/// # Invariants
/// The minimum and maximum are guaranteed to have the same logical type.
#[derive(Debug, PartialEq)]
pub struct ColumnIndex {
    /// The minimum values in the pages
    pub min: Box<dyn Array>,
    /// The maximum values in the pages
    pub max: Box<dyn Array>,
    /// The number of null values in the pages. A [`UInt64Array`] for non-nested types
    pub null_count: Box<dyn Array>,
}

impl ColumnIndex {
    /// The [`DataType`] of the column index.
    pub fn data_type(&self) -> &DataType {
        self.min.data_type()
    }
}

/// Given a sequence of [`ParquetIndex`] representing the page indexes of each column in the
/// parquet file, returns the page-level statistics as arrow's arrays, as a vector of [`ColumnIndex`].
///
/// This function maps timestamps, decimal types, etc. accordingly.
/// # Implementation
/// This function is CPU-bounded `O(P)` where `P` is the total number of pages in all columns.
/// # Error
/// This function errors iff the value is not deserializable to arrow (e.g. invalid utf-8)
fn deserialize(
    indexes: &mut VecDeque<&Box<dyn ParquetIndex>>,
    data_type: DataType,
) -> Result<ColumnIndex, Error> {
    match data_type.to_physical_type() {
        PhysicalType::Boolean => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanIndex>()
                .unwrap();
            Ok(boolean::deserialize(&index.indexes))
        }
        PhysicalType::Primitive(PrimitiveType::Int128) => {
            let index = indexes.pop_front().unwrap();
            match index.physical_type() {
                ParquetPhysicalType::Int32 => {
                    let index = index.as_any().downcast_ref::<NativeIndex<i32>>().unwrap();
                    Ok(primitive::deserialize_i32(&index.indexes, data_type))
                }
                parquet2::schema::types::PhysicalType::Int64 => {
                    let index = index.as_any().downcast_ref::<NativeIndex<i64>>().unwrap();
                    Ok(primitive::deserialize_i64(
                        &index.indexes,
                        &index.primitive_type,
                        data_type,
                    ))
                }
                parquet2::schema::types::PhysicalType::FixedLenByteArray(_) => {
                    let index = index.as_any().downcast_ref::<FixedLenByteIndex>().unwrap();
                    Ok(fixed_len_binary::deserialize(&index.indexes, data_type))
                }
                other => Err(Error::nyi(format!(
                    "Deserialize {other:?} to arrow's int64"
                ))),
            }
        }
        PhysicalType::Primitive(PrimitiveType::UInt8)
        | PhysicalType::Primitive(PrimitiveType::UInt16)
        | PhysicalType::Primitive(PrimitiveType::UInt32)
        | PhysicalType::Primitive(PrimitiveType::Int32) => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<NativeIndex<i32>>()
                .unwrap();
            Ok(primitive::deserialize_i32(&index.indexes, data_type))
        }
        PhysicalType::Primitive(PrimitiveType::Int64) => {
            let index = indexes.pop_front().unwrap();
            match index.physical_type() {
                ParquetPhysicalType::Int64 => {
                    let index = index.as_any().downcast_ref::<NativeIndex<i64>>().unwrap();
                    Ok(primitive::deserialize_i64(
                        &index.indexes,
                        &index.primitive_type,
                        data_type,
                    ))
                }
                parquet2::schema::types::PhysicalType::Int96 => {
                    let index = index
                        .as_any()
                        .downcast_ref::<NativeIndex<[u32; 3]>>()
                        .unwrap();
                    Ok(primitive::deserialize_i96(&index.indexes, data_type))
                }
                other => Err(Error::nyi(format!(
                    "Deserialize {other:?} to arrow's int64"
                ))),
            }
        }
        PhysicalType::Primitive(PrimitiveType::Float32) => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<NativeIndex<f32>>()
                .unwrap();
            Ok(primitive::deserialize_id(&index.indexes, data_type))
        }
        PhysicalType::Primitive(PrimitiveType::Float64) => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<NativeIndex<f64>>()
                .unwrap();
            Ok(primitive::deserialize_id(&index.indexes, data_type))
        }
        PhysicalType::Binary
        | PhysicalType::LargeBinary
        | PhysicalType::Utf8
        | PhysicalType::LargeUtf8 => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<ByteIndex>()
                .unwrap();
            binary::deserialize(&index.indexes, &data_type)
        }
        PhysicalType::FixedSizeBinary => {
            let index = indexes
                .pop_front()
                .unwrap()
                .as_any()
                .downcast_ref::<FixedLenByteIndex>()
                .unwrap();
            Ok(fixed_len_binary::deserialize(&index.indexes, data_type))
        }
        PhysicalType::Dictionary(_) => {
            if let DataType::Dictionary(key, inner, is_sorted) = data_type.to_logical_type() {
                let child = deserialize(indexes, (**inner).clone())?;
                Ok(match_integer_type!(key, |$T| {
                    let keys: Vec<$T> = (0..child.min.len() as $T).collect();
                    let keys = PrimitiveArray::<$T>::from_vec(keys);
                    ColumnIndex {
                        min: DictionaryArray::<$T>::try_new(data_type.clone(), keys.clone(), child.min).map(|x|x.boxed())?,
                        max: DictionaryArray::<$T>::try_new(data_type.clone(), keys.clone(), child.max).map(|x|x.boxed())?,
                        null_count: DictionaryArray::<$T>::try_new(DataType::Dictionary(*key, Box::new(child.null_count.data_type().clone()), *is_sorted), keys, child.null_count).map(|x|x.boxed())?,
                    }
                }))
            } else {
                unreachable!()
            }
        }
        PhysicalType::List => {
            let inner = if let DataType::List(inner) = data_type.to_logical_type() {
                inner
            } else {
                unreachable!()
            };
            let child = deserialize(indexes, inner.data_type.clone())?;
            let offsets = vec![0, child.min.len() as i32];
            Ok(ColumnIndex {
                min: ListArray::<i32>::new(
                    data_type.clone(),
                    offsets.clone().into(),
                    child.min,
                    None,
                )
                .boxed(),
                max: ListArray::<i32>::new(
                    data_type.clone(),
                    offsets.clone().into(),
                    child.max,
                    None,
                )
                .boxed(),
                null_count: ListArray::<i32>::new(
                    DataType::List({
                        let mut inner = (**inner).clone();
                        inner.data_type = child.null_count.data_type().clone();
                        Box::new(inner)
                    }),
                    offsets.into(),
                    child.null_count,
                    None,
                )
                .boxed(),
            })
        }
        PhysicalType::LargeList => {
            let inner = if let DataType::LargeList(inner) = data_type.to_logical_type() {
                inner
            } else {
                unreachable!()
            };
            let child = deserialize(indexes, inner.data_type.clone())?;
            let offsets = vec![0, child.min.len() as i64];
            Ok(ColumnIndex {
                min: ListArray::<i64>::new(
                    data_type.clone(),
                    offsets.clone().into(),
                    child.min,
                    None,
                )
                .boxed(),
                max: ListArray::<i64>::new(
                    data_type.clone(),
                    offsets.clone().into(),
                    child.max,
                    None,
                )
                .boxed(),
                null_count: ListArray::<i64>::new(
                    DataType::LargeList({
                        let mut inner = (**inner).clone();
                        inner.data_type = child.null_count.data_type().clone();
                        Box::new(inner)
                    }),
                    offsets.into(),
                    child.null_count,
                    None,
                )
                .boxed(),
            })
        }
        PhysicalType::Struct => {
            let children_fields = if let DataType::Struct(children) = data_type.to_logical_type() {
                children
            } else {
                unreachable!()
            };
            let children = children_fields
                .iter()
                .map(|child| deserialize(indexes, child.data_type.clone()))
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(ColumnIndex {
                min: StructArray::new(
                    data_type.clone(),
                    children.iter().map(|child| child.min.clone()).collect(),
                    None,
                )
                .boxed(),
                max: StructArray::new(
                    data_type.clone(),
                    children.iter().map(|child| child.max.clone()).collect(),
                    None,
                )
                .boxed(),
                null_count: StructArray::new(
                    DataType::Struct({
                        let mut children_fields = children_fields.clone();
                        children_fields.iter_mut().zip(children.iter()).for_each(
                            |(child_field, child)| {
                                child_field.data_type = child.null_count.data_type().clone();
                            },
                        );
                        children_fields
                    }),
                    children
                        .iter()
                        .map(|child| child.null_count.clone())
                        .collect(),
                    None,
                )
                .boxed(),
            })
        }

        other => Err(Error::nyi(format!(
            "Deserialize into arrow's {other:?} page index"
        ))),
    }
}

/// Checks whether the row groups have page index information
pub fn has_indexes(row_groups: &[RowGroupMetaData]) -> bool {
    row_groups.iter().all(|group| {
        group
            .columns()
            .iter()
            .all(|chunk| chunk.column_chunk().column_index_offset.is_some())
    })
}

/// Reads the column indexes from the reader assuming a valid set of derived Arrow fields
/// for all parquet the columns in the file.
///
/// It returns one [`ColumnIndex`] per field in `fields`
///
/// This function is expected to be used to filter out parquet pages.
///
/// # Implementation
/// This function is IO-bounded and calls `reader.read_exact` exactly once.
/// # Error
/// Errors iff the indexes can't be read or their deserialization to arrow is incorrect (e.g. invalid utf-8)
pub fn read_columns_indexes<R: Read + Seek>(
    reader: &mut R,
    chunks: &[ColumnChunkMetaData],
    fields: &[Field],
) -> Result<Vec<ColumnIndex>, Error> {
    let indexes = _read_columns_indexes(reader, chunks)?;

    fields
        .iter()
        .map(|field| {
            let indexes = get_field_pages(chunks, &indexes, &field.name);
            let mut indexes = indexes.into_iter().collect();

            deserialize(&mut indexes, field.data_type.clone())
        })
        .collect()
}
