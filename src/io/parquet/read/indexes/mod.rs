use parquet2::indexes::{
    BooleanIndex, ByteIndex, FixedLenByteIndex, Index as ParquetIndex, NativeIndex,
};
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::read::read_columns_indexes as _read_columns_indexes;
use parquet2::schema::types::PhysicalType as ParquetPhysicalType;

mod binary;
mod boolean;
mod fixed_len_binary;
mod primitive;

use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::datatypes::{Field, PrimitiveType};
use crate::{
    array::{Array, UInt64Array},
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
    /// The number of null values in the pages
    pub null_count: UInt64Array,
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
    mut indexes: VecDeque<&Box<dyn ParquetIndex>>,
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
        PhysicalType::Primitive(PrimitiveType::Int32) => {
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
        _ => todo!(),
    }
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
            let indexes = indexes.into_iter().collect();

            deserialize(indexes, field.data_type.clone())
        })
        .collect()
}
