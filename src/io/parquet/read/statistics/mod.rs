//! APIs exposing `parquet2`'s statistics as arrow's statistics.
use std::any::Any;

use parquet2::metadata::ColumnChunkMetaData;
use parquet2::schema::types::PhysicalType;
use parquet2::statistics::PrimitiveStatistics as ParquetPrimitiveStatistics;
use parquet2::statistics::Statistics as ParquetStatistics;

use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::error::ArrowError;
use crate::error::Result;

mod primitive;
pub use primitive::*;
mod binary;
pub use binary::*;
mod boolean;
pub use boolean::*;
mod fixlen;
pub use fixlen::*;

use super::get_field_columns;

/// Trait representing a deserialized parquet statistics into arrow.
pub trait Statistics: std::fmt::Debug {
    /// returns the [`DataType`] of the statistics.
    fn data_type(&self) -> &DataType;

    /// Returns `dyn Any` can used to downcast to a physical type.
    fn as_any(&self) -> &dyn Any;

    /// Return the null count statistic
    fn null_count(&self) -> Option<i64>;
}

impl PartialEq for &dyn Statistics {
    fn eq(&self, other: &Self) -> bool {
        self.data_type() == other.data_type()
    }
}

impl PartialEq for Box<dyn Statistics> {
    fn eq(&self, other: &Self) -> bool {
        self.data_type() == other.data_type()
    }
}

/// Deserializes [`ParquetStatistics`] into [`Statistics`] based on `data_type`.
/// This takes into account the Arrow schema declared in Parquet's schema
fn _deserialize_statistics(
    stats: &dyn ParquetStatistics,
    data_type: DataType,
) -> Result<Box<dyn Statistics>> {
    match stats.physical_type() {
        PhysicalType::Int32 => {
            let stats = stats.as_any().downcast_ref().unwrap();
            primitive::statistics_from_i32(stats, data_type)
        }
        PhysicalType::Int64 => {
            let stats = stats.as_any().downcast_ref().unwrap();
            primitive::statistics_from_i64(stats, data_type)
        }
        PhysicalType::ByteArray => {
            let stats = stats.as_any().downcast_ref().unwrap();
            binary::statistics_from_byte_array(stats, data_type)
        }
        PhysicalType::Boolean => {
            let stats = stats.as_any().downcast_ref().unwrap();
            Ok(Box::new(BooleanStatistics::from(stats)))
        }
        PhysicalType::Float => {
            let stats = stats
                .as_any()
                .downcast_ref::<ParquetPrimitiveStatistics<f32>>()
                .unwrap();
            Ok(Box::new(PrimitiveStatistics::<f32>::from((
                stats, data_type,
            ))))
        }
        PhysicalType::Double => {
            let stats = stats
                .as_any()
                .downcast_ref::<ParquetPrimitiveStatistics<f64>>()
                .unwrap();
            Ok(Box::new(PrimitiveStatistics::<f64>::from((
                stats, data_type,
            ))))
        }
        PhysicalType::FixedLenByteArray(_) => {
            let stats = stats.as_any().downcast_ref().unwrap();
            fixlen::statistics_from_fix_len(stats, data_type)
        }
        _ => Err(ArrowError::NotYetImplemented(
            "Reading Fixed-len array statistics is not yet supported".to_string(),
        )),
    }
}

fn get_fields(field: &Field) -> Vec<&Field> {
    match field.data_type.to_logical_type() {
        DataType::List(inner) => get_fields(inner),
        DataType::LargeList(inner) => get_fields(inner),
        DataType::Struct(fields) => fields.iter().map(get_fields).flatten().collect(),
        _ => vec![field],
    }
}

/// Deserializes [`ParquetStatistics`] into [`Statistics`] associated to `field`
///
/// For non-nested types, it returns a single column.
/// For nested types, it returns one column per parquet primitive column.
pub fn deserialize_statistics(
    field: &Field,
    columns: &[ColumnChunkMetaData],
) -> Result<Vec<Option<Box<dyn Statistics>>>> {
    let columns = get_field_columns(columns, field.name.as_ref());

    let fields = get_fields(field);

    columns
        .into_iter()
        .zip(fields.into_iter())
        .map(|(column, field)| {
            column
                .statistics()
                .map(|x| _deserialize_statistics(x?.as_ref(), field.data_type.clone()))
                .transpose()
        })
        .collect()
}
