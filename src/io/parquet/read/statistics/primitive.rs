use crate::{datatypes::DataType, types::NativeType};
use parquet2::schema::types::ParquetType;
use parquet2::statistics::PrimitiveStatistics as ParquetPrimitiveStatistics;
use parquet2::types::NativeType as ParquetNativeType;

use super::super::schema;
use super::Statistics;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveStatistics<T: NativeType> {
    pub data_type: DataType,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
}

impl<T: NativeType> Statistics for PrimitiveStatistics<T> {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl<T, R> From<(&ParquetPrimitiveStatistics<R>, DataType)> for PrimitiveStatistics<T>
where
    T: NativeType,
    R: ParquetNativeType,
    R: num::cast::AsPrimitive<T>,
{
    fn from((stats, data_type): (&ParquetPrimitiveStatistics<R>, DataType)) -> Self {
        Self {
            data_type,
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats.min_value.map(|x| x.as_()),
            max_value: stats.max_value.map(|x| x.as_()),
        }
    }
}

pub(super) fn statistics_from_i32(
    stats: &ParquetPrimitiveStatistics<i32>,
    type_: &ParquetType,
) -> Result<Box<dyn Statistics>> {
    let data_type = schema::to_data_type(type_)?.unwrap();

    use DataType::*;
    Ok(match data_type {
        UInt8 => {
            Box::new(PrimitiveStatistics::<u8>::from((stats, data_type))) as Box<dyn Statistics>
        }
        UInt16 => Box::new(PrimitiveStatistics::<u16>::from((stats, data_type))),
        UInt32 => Box::new(PrimitiveStatistics::<u32>::from((stats, data_type))),
        Int8 => Box::new(PrimitiveStatistics::<i8>::from((stats, data_type))),
        Int16 => Box::new(PrimitiveStatistics::<i16>::from((stats, data_type))),
        _ => Box::new(PrimitiveStatistics::<i32>::from((stats, data_type))),
    })
}

pub(super) fn statistics_from_i64(
    stats: &ParquetPrimitiveStatistics<i64>,
    type_: &ParquetType,
) -> Result<Box<dyn Statistics>> {
    let data_type = schema::to_data_type(type_)?.unwrap();

    use DataType::*;
    Ok(match data_type {
        UInt64 => {
            Box::new(PrimitiveStatistics::<u64>::from((stats, data_type))) as Box<dyn Statistics>
        }
        _ => Box::new(PrimitiveStatistics::<i64>::from((stats, data_type))),
    })
}
