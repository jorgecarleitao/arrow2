use std::any::Any;

use parquet2::schema::types::{PrimitiveLogicalType, PrimitiveType, TimeUnit as ParquetTimeUnit};
use parquet2::statistics::PrimitiveStatistics as ParquetPrimitiveStatistics;
use parquet2::types::NativeType as ParquetNativeType;

use crate::datatypes::TimeUnit;
use crate::error::Result;
use crate::{datatypes::DataType, types::NativeType};

use super::Statistics;

/// Arrow-deserialized parquet Statistics of a primitive type
#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveStatistics<T: NativeType> {
    /// the data type
    pub data_type: DataType,
    /// number of nulls
    pub null_count: Option<i64>,
    /// number of dictinct values
    pub distinct_count: Option<i64>,
    /// Minimum
    pub min_value: Option<T>,
    /// Maximum
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

fn timestamp(type_: &PrimitiveType, time_unit: TimeUnit, x: i64) -> i64 {
    let unit = if let Some(PrimitiveLogicalType::Timestamp { unit, .. }) = &type_.logical_type {
        unit
    } else {
        return x;
    };

    match (unit, time_unit) {
        (ParquetTimeUnit::Milliseconds, TimeUnit::Second) => x / 1_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Second) => x / 1_000_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Second) => x * 1_000_000_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Millisecond) => x,
        (ParquetTimeUnit::Microseconds, TimeUnit::Millisecond) => x / 1_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Millisecond) => x / 1_000_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Microsecond) => x * 1_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Microsecond) => x,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Microsecond) => x / 1_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Nanosecond) => x * 1_000_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Nanosecond) => x * 1_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Nanosecond) => x,
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
                .map(|x| timestamp(&stats.primitive_type, time_unit, x)),
            max_value: stats
                .max_value
                .map(|x| timestamp(&stats.primitive_type, time_unit, x)),
        }),
        Decimal(_, _) => Box::new(PrimitiveStatistics::<i128>::from((stats, data_type))),
        _ => Box::new(PrimitiveStatistics::<i64>::from((stats, data_type))),
    })
}
