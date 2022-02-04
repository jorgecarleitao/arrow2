use crate::datatypes::TimeUnit;
use crate::{datatypes::DataType, types::NativeType};
use parquet2::schema::types::{
    LogicalType, ParquetType, TimeUnit as ParquetTimeUnit, TimestampType,
};
use parquet2::statistics::PrimitiveStatistics as ParquetPrimitiveStatistics;
use parquet2::types::NativeType as ParquetNativeType;
use std::any::Any;

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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

impl<T, R> From<(&ParquetPrimitiveStatistics<R>, DataType)> for PrimitiveStatistics<T>
where
    T: NativeType,
    R: ParquetNativeType,
    R: num_traits::AsPrimitive<T>,
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
    data_type: DataType,
) -> Result<Box<dyn Statistics>> {
    use DataType::*;
    Ok(match data_type {
        UInt8 => {
            Box::new(PrimitiveStatistics::<u8>::from((stats, data_type))) as Box<dyn Statistics>
        }
        UInt16 => Box::new(PrimitiveStatistics::<u16>::from((stats, data_type))),
        UInt32 => Box::new(PrimitiveStatistics::<u32>::from((stats, data_type))),
        Int8 => Box::new(PrimitiveStatistics::<i8>::from((stats, data_type))),
        Int16 => Box::new(PrimitiveStatistics::<i16>::from((stats, data_type))),
        Decimal(_, _) => Box::new(PrimitiveStatistics::<i128>::from((stats, data_type))),
        _ => Box::new(PrimitiveStatistics::<i32>::from((stats, data_type))),
    })
}

fn timestamp(type_: &ParquetType, time_unit: TimeUnit, x: i64) -> i64 {
    let logical_type = if let ParquetType::PrimitiveType { logical_type, .. } = type_ {
        logical_type
    } else {
        unreachable!()
    };

    let unit = if let Some(LogicalType::TIMESTAMP(TimestampType { unit, .. })) = logical_type {
        unit
    } else {
        return x;
    };

    match (unit, time_unit) {
        (ParquetTimeUnit::MILLIS(_), TimeUnit::Second) => x / 1_000,
        (ParquetTimeUnit::MICROS(_), TimeUnit::Second) => x / 1_000_000,
        (ParquetTimeUnit::NANOS(_), TimeUnit::Second) => x * 1_000_000_000,

        (ParquetTimeUnit::MILLIS(_), TimeUnit::Millisecond) => x,
        (ParquetTimeUnit::MICROS(_), TimeUnit::Millisecond) => x / 1_000,
        (ParquetTimeUnit::NANOS(_), TimeUnit::Millisecond) => x / 1_000_000,

        (ParquetTimeUnit::MILLIS(_), TimeUnit::Microsecond) => x * 1_000,
        (ParquetTimeUnit::MICROS(_), TimeUnit::Microsecond) => x,
        (ParquetTimeUnit::NANOS(_), TimeUnit::Microsecond) => x / 1_000,

        (ParquetTimeUnit::MILLIS(_), TimeUnit::Nanosecond) => x * 1_000_000,
        (ParquetTimeUnit::MICROS(_), TimeUnit::Nanosecond) => x * 1_000,
        (ParquetTimeUnit::NANOS(_), TimeUnit::Nanosecond) => x,
    }
}

pub(super) fn statistics_from_i64(
    stats: &ParquetPrimitiveStatistics<i64>,
    data_type: DataType,
) -> Result<Box<dyn Statistics>> {
    use DataType::*;
    Ok(match data_type {
        UInt64 => {
            Box::new(PrimitiveStatistics::<u64>::from((stats, data_type))) as Box<dyn Statistics>
        }
        Timestamp(time_unit, None) => Box::new(PrimitiveStatistics::<i64> {
            data_type,
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats
                .min_value
                .map(|x| timestamp(stats.descriptor.type_(), time_unit, x)),
            max_value: stats
                .max_value
                .map(|x| timestamp(stats.descriptor.type_(), time_unit, x)),
        }),
        Decimal(_, _) => Box::new(PrimitiveStatistics::<i128>::from((stats, data_type))),
        _ => Box::new(PrimitiveStatistics::<i64>::from((stats, data_type))),
    })
}
