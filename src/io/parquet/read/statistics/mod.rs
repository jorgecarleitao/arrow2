//! APIs exposing `parquet2`'s statistics as arrow's statistics.
use std::collections::VecDeque;
use std::sync::Arc;

use parquet2::metadata::RowGroupMetaData;
use parquet2::schema::types::{
    PhysicalType as ParquetPhysicalType, PrimitiveType as ParquetPrimitiveType,
};
use parquet2::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics,
    Statistics as ParquetStatistics,
};

use crate::array::*;
use crate::datatypes::IntervalUnit;
use crate::datatypes::{DataType, Field, PhysicalType};
use crate::error::ArrowError;
use crate::error::Result;

mod binary;
mod boolean;
mod dictionary;
mod fixlen;
mod list;
mod primitive;
mod utf8;

use self::list::DynMutableListArray;

use super::get_field_columns;

/// Arrow-deserialized parquet Statistics of a file
#[derive(Debug, PartialEq)]
pub struct Statistics {
    /// number of nulls
    pub null_count: UInt64Array,
    /// number of dictinct values
    pub distinct_count: UInt64Array,
    /// Minimum
    pub min_value: Box<dyn Array>,
    /// Maximum
    pub max_value: Box<dyn Array>,
}

/// Arrow-deserialized parquet Statistics of a file
#[derive(Debug)]
struct MutableStatistics {
    /// number of nulls
    pub null_count: UInt64Vec,
    /// number of dictinct values
    pub distinct_count: UInt64Vec,
    /// Minimum
    pub min_value: Box<dyn MutableArray>,
    /// Maximum
    pub max_value: Box<dyn MutableArray>,
}

impl From<MutableStatistics> for Statistics {
    fn from(mut s: MutableStatistics) -> Self {
        Self {
            null_count: s.null_count.into(),
            distinct_count: s.distinct_count.into(),
            min_value: s.min_value.as_box(),
            max_value: s.max_value.as_box(),
        }
    }
}

fn make_mutable(data_type: &DataType, capacity: usize) -> Result<Box<dyn MutableArray>> {
    Ok(match data_type.to_physical_type() {
        PhysicalType::Boolean => {
            Box::new(MutableBooleanArray::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            Box::new(MutablePrimitiveArray::<$T>::with_capacity(capacity).to(data_type.clone()))
                as Box<dyn MutableArray>
        }),
        PhysicalType::Binary => {
            Box::new(MutableBinaryArray::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::LargeBinary => {
            Box::new(MutableBinaryArray::<i64>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::Utf8 => {
            Box::new(MutableUtf8Array::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::LargeUtf8 => {
            Box::new(MutableUtf8Array::<i64>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::FixedSizeBinary => Box::new(MutableFixedSizeBinaryArray::from_data(
            data_type.clone(),
            vec![],
            None,
        )) as _,
        PhysicalType::LargeList | PhysicalType::List => Box::new(
            DynMutableListArray::try_with_capacity(data_type.clone(), capacity)?,
        ) as Box<dyn MutableArray>,
        PhysicalType::Dictionary(_) => Box::new(
            dictionary::DynMutableDictionary::try_with_capacity(data_type.clone(), capacity)?,
        ),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Deserializing parquet stats from {:?} is still not implemented",
                other
            )))
        }
    })
}

impl MutableStatistics {
    fn try_new(field: &Field) -> Result<Self> {
        let min_value = make_mutable(&field.data_type, 0)?;
        let max_value = make_mutable(&field.data_type, 0)?;

        Ok(Self {
            null_count: UInt64Vec::new(),
            distinct_count: UInt64Vec::new(),
            min_value,
            max_value,
        })
    }
}

fn push_others(
    from: Option<&dyn ParquetStatistics>,
    distinct_count: &mut UInt64Vec,
    null_count: &mut UInt64Vec,
) {
    let from = if let Some(from) = from {
        from
    } else {
        distinct_count.push(None);
        null_count.push(None);
        return;
    };
    let (distinct, null_count1) = match from.physical_type() {
        ParquetPhysicalType::Boolean => {
            let from = from.as_any().downcast_ref::<BooleanStatistics>().unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::Int32 => {
            let from = from
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::Int64 => {
            let from = from
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::Int96 => {
            let from = from
                .as_any()
                .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                .unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::Float => {
            let from = from
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::Double => {
            let from = from
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::ByteArray => {
            let from = from.as_any().downcast_ref::<BinaryStatistics>().unwrap();
            (from.distinct_count, from.null_count)
        }
        ParquetPhysicalType::FixedLenByteArray(_) => {
            let from = from.as_any().downcast_ref::<FixedLenStatistics>().unwrap();
            (from.distinct_count, from.null_count)
        }
    };

    distinct_count.push(distinct.map(|x| x as u64));
    null_count.push(null_count1.map(|x| x as u64));
}

fn push(
    mut stats: VecDeque<(Option<Arc<dyn ParquetStatistics>>, ParquetPrimitiveType)>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
    distinct_count: &mut UInt64Vec,
    null_count: &mut UInt64Vec,
) -> Result<()> {
    match min.data_type().to_logical_type() {
        List(_) | LargeList(_) => {
            let min = min
                .as_mut_any()
                .downcast_mut::<list::DynMutableListArray>()
                .unwrap();
            let max = max
                .as_mut_any()
                .downcast_mut::<list::DynMutableListArray>()
                .unwrap();
            return push(
                stats,
                min.inner.as_mut(),
                max.inner.as_mut(),
                distinct_count,
                null_count,
            );
        }
        Dictionary(_, _, _) => {
            let min = min
                .as_mut_any()
                .downcast_mut::<dictionary::DynMutableDictionary>()
                .unwrap();
            let max = max
                .as_mut_any()
                .downcast_mut::<dictionary::DynMutableDictionary>()
                .unwrap();
            return push(
                stats,
                min.inner.as_mut(),
                max.inner.as_mut(),
                distinct_count,
                null_count,
            );
        }
        _ => {}
    }

    let (from, type_) = stats.pop_front().unwrap();
    let from = from.as_deref();

    push_others(from, distinct_count, null_count);

    let physical_type = &type_.physical_type;

    use DataType::*;
    match min.data_type().to_logical_type() {
        Boolean => boolean::push(from, min, max),
        Int8 => primitive::push(from, min, max, |x: i32| Ok(x as i8)),
        Int16 => primitive::push(from, min, max, |x: i32| Ok(x as i16)),
        Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::push(from, min, max, |x: i32| Ok(x as i32))
        }
        UInt8 => primitive::push(from, min, max, |x: i32| Ok(x as u8)),
        UInt16 => primitive::push(from, min, max, |x: i32| Ok(x as u16)),
        UInt32 => primitive::push(from, min, max, |x: i32| Ok(x as u32)),
        Int32 => primitive::push(from, min, max, |x: i32| Ok(x as i32)),
        Int64 | Date64 | Time64(_) | Duration(_) => {
            primitive::push(from, min, max, |x: i64| Ok(x as i64))
        }
        UInt64 => primitive::push(from, min, max, |x: i64| Ok(x as u64)),
        Timestamp(time_unit, _) => {
            let time_unit = *time_unit;
            primitive::push(from, min, max, |x: i64| {
                Ok(primitive::timestamp(
                    type_.logical_type.as_ref(),
                    time_unit,
                    x,
                ))
            })
        }
        Float32 => primitive::push(from, min, max, |x: f32| Ok(x as f32)),
        Float64 => primitive::push(from, min, max, |x: f64| Ok(x as f64)),
        Decimal(_, _) => match physical_type {
            ParquetPhysicalType::Int32 => primitive::push(from, min, max, |x: i32| Ok(x as i128)),
            ParquetPhysicalType::Int64 => primitive::push(from, min, max, |x: i64| Ok(x as i128)),
            ParquetPhysicalType::FixedLenByteArray(n) if *n > 16 => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Can't decode Decimal128 type from Fixed Size Byte Array of len {:?}",
                    n
                )))
            }
            ParquetPhysicalType::FixedLenByteArray(n) => fixlen::push_i128(from, *n, min, max),
            _ => unreachable!(),
        },
        Binary => binary::push::<i32>(from, min, max),
        LargeBinary => binary::push::<i64>(from, min, max),
        Utf8 => utf8::push::<i32>(from, min, max),
        LargeUtf8 => utf8::push::<i64>(from, min, max),
        FixedSizeBinary(_) => fixlen::push(from, min, max),
        other => todo!("{:?}", other),
    }
}

/// Deserializes [`ParquetStatistics`] into [`Statistics`] associated to `field`
///
/// For non-nested types, it returns a single column.
/// For nested types, it returns one column per parquet primitive column.
pub fn deserialize_statistics(field: &Field, groups: &[RowGroupMetaData]) -> Result<Statistics> {
    if groups.is_empty() {
        todo!("Return an empty statistics")
    }

    let mut statistics = MutableStatistics::try_new(field)?;

    // transpose
    groups.iter().try_for_each(|group| {
        let columns = get_field_columns(group.columns(), field.name.as_ref());
        let stats = columns
            .into_iter()
            .map(|column| {
                Ok((
                    column.statistics().transpose()?,
                    column.descriptor().descriptor.primitive_type.clone(),
                ))
            })
            .collect::<Result<VecDeque<(Option<_>, ParquetPrimitiveType)>>>()?;
        push(
            stats,
            statistics.min_value.as_mut(),
            statistics.max_value.as_mut(),
            &mut statistics.distinct_count,
            &mut statistics.null_count,
        )
    })?;

    Ok(statistics.into())
}
