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

use std::io::{Read, Seek};

use crate::datatypes::Field;
use crate::{
    array::{Array, UInt64Array},
    datatypes::DataType,
    error::ArrowError,
};

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
/// This function is CPU-bounded but `O(P)` where `P` is the total number of pages in all columns.
/// # Error
/// This function errors iff the value is not deserializable to arrow (e.g. invalid utf-8)
fn deserialize(
    indexes: &[Box<dyn ParquetIndex>],
    data_types: Vec<DataType>,
) -> Result<Vec<ColumnIndex>, ArrowError> {
    indexes
        .iter()
        .zip(data_types.into_iter())
        .map(|(index, data_type)| match index.physical_type() {
            ParquetPhysicalType::Boolean => {
                let index = index.as_any().downcast_ref::<BooleanIndex>().unwrap();
                Ok(boolean::deserialize(&index.indexes))
            }
            ParquetPhysicalType::Int32 => {
                let index = index.as_any().downcast_ref::<NativeIndex<i32>>().unwrap();
                Ok(primitive::deserialize_i32(&index.indexes, data_type))
            }
            ParquetPhysicalType::Int64 => {
                let index = index.as_any().downcast_ref::<NativeIndex<i64>>().unwrap();
                Ok(primitive::deserialize_i64(
                    &index.indexes,
                    &index.primitive_type,
                    data_type,
                ))
            }
            ParquetPhysicalType::Int96 => {
                let index = index
                    .as_any()
                    .downcast_ref::<NativeIndex<[u32; 3]>>()
                    .unwrap();
                Ok(primitive::deserialize_i96(&index.indexes, data_type))
            }
            ParquetPhysicalType::Float => {
                let index = index.as_any().downcast_ref::<NativeIndex<f32>>().unwrap();
                Ok(primitive::deserialize_id(&index.indexes, data_type))
            }
            ParquetPhysicalType::Double => {
                let index = index.as_any().downcast_ref::<NativeIndex<f64>>().unwrap();
                Ok(primitive::deserialize_id(&index.indexes, data_type))
            }
            ParquetPhysicalType::ByteArray => {
                let index = index.as_any().downcast_ref::<ByteIndex>().unwrap();
                binary::deserialize(&index.indexes, &data_type)
            }
            ParquetPhysicalType::FixedLenByteArray(_) => {
                let index = index.as_any().downcast_ref::<FixedLenByteIndex>().unwrap();
                Ok(fixed_len_binary::deserialize(&index.indexes, data_type))
            }
        })
        .collect()
}

// recursive function to get the corresponding leaf data_types corresponding to the
// parquet columns
fn populate_dt(data_type: &DataType, container: &mut Vec<DataType>) {
    match data_type.to_logical_type() {
        DataType::List(inner) => populate_dt(&inner.data_type, container),
        DataType::LargeList(inner) => populate_dt(&inner.data_type, container),
        DataType::Dictionary(_, inner, _) => populate_dt(inner, container),
        DataType::Struct(fields) => fields
            .iter()
            .for_each(|f| populate_dt(&f.data_type, container)),
        _ => container.push(data_type.clone()),
    }
}

/// Reads the column indexes from the reader assuming a valid set of derived Arrow fields
/// for all parquet the columns in the file.
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
) -> Result<Vec<ColumnIndex>, ArrowError> {
    let indexes = _read_columns_indexes(reader, chunks)?;

    // map arrow fields to the corresponding columns in parquet taking into account
    // that fields may be nested but parquet column indexes are only leaf columns
    let mut data_types = vec![];
    fields
        .iter()
        .map(|f| &f.data_type)
        .for_each(|d| populate_dt(d, &mut data_types));

    deserialize(&indexes, data_types)
}
