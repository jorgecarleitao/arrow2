use std::fmt::{Debug, Formatter, Result, Write};

use crate::array::Array;
use crate::datatypes::{IntervalUnit, TimeUnit};
use crate::types::{days_ms, months_days_ns};

use super::super::super::temporal_conversions;
use super::super::super::types::NativeType;
use super::super::fmt::write_vec;
use super::PrimitiveArray;

macro_rules! dyn_primitive {
    ($array:expr, $ty:ty, $expr:expr) => {{
        let array = ($array as &dyn Array)
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Box::new(move |f, index| write!(f, "{}", $expr(array.value(index))))
    }};
}

pub fn get_write_value<'a, T: NativeType, F: Write>(
    array: &'a PrimitiveArray<T>,
) -> Box<dyn Fn(&mut F, usize) -> Result + 'a> {
    use crate::datatypes::DataType::*;
    match array.data_type().to_logical_type() {
        Int8 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Int16 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Int32 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Int64 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        UInt8 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        UInt16 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        UInt32 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        UInt64 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Float16 => unreachable!(),
        Float32 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Float64 => Box::new(|f, index| write!(f, "{}", array.value(index))),
        Date32 => {
            dyn_primitive!(array, i32, temporal_conversions::date32_to_date)
        }
        Date64 => {
            dyn_primitive!(array, i64, temporal_conversions::date64_to_date)
        }
        Time32(TimeUnit::Second) => {
            dyn_primitive!(array, i32, temporal_conversions::time32s_to_time)
        }
        Time32(TimeUnit::Millisecond) => {
            dyn_primitive!(array, i32, temporal_conversions::time32ms_to_time)
        }
        Time32(_) => unreachable!(), // remaining are not valid
        Time64(TimeUnit::Microsecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64us_to_time)
        }
        Time64(TimeUnit::Nanosecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64ns_to_time)
        }
        Time64(_) => unreachable!(), // remaining are not valid
        Timestamp(time_unit, tz) => {
            if let Some(tz) = tz {
                let timezone = temporal_conversions::parse_offset(tz);
                match timezone {
                    Ok(timezone) => {
                        dyn_primitive!(array, i64, |time| {
                            temporal_conversions::timestamp_to_datetime(time, *time_unit, &timezone)
                        })
                    }
                    #[cfg(feature = "chrono-tz")]
                    Err(_) => {
                        let timezone = temporal_conversions::parse_offset_tz(tz).unwrap();
                        dyn_primitive!(array, i64, |time| {
                            temporal_conversions::timestamp_to_datetime(time, *time_unit, &timezone)
                        })
                    }
                    #[cfg(not(feature = "chrono-tz"))]
                    _ => panic!(
                        "Invalid Offset format (must be [-]00:00) or chrono-tz feature not active"
                    ),
                }
            } else {
                dyn_primitive!(array, i64, |time| {
                    temporal_conversions::timestamp_to_naive_datetime(time, *time_unit)
                })
            }
        }
        Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(array, i32, |x| format!("{}m", x))
        }
        Interval(IntervalUnit::DayTime) => {
            dyn_primitive!(array, days_ms, |x: days_ms| format!(
                "{}d{}ms",
                x.days(),
                x.milliseconds()
            ))
        }
        Interval(IntervalUnit::MonthDayNano) => {
            dyn_primitive!(array, months_days_ns, |x: months_days_ns| format!(
                "{}m{}d{}ns",
                x.months(),
                x.days(),
                x.ns()
            ))
        }
        Duration(TimeUnit::Second) => dyn_primitive!(array, i64, |x| format!("{}s", x)),
        Duration(TimeUnit::Millisecond) => dyn_primitive!(array, i64, |x| format!("{}ms", x)),
        Duration(TimeUnit::Microsecond) => dyn_primitive!(array, i64, |x| format!("{}us", x)),
        Duration(TimeUnit::Nanosecond) => dyn_primitive!(array, i64, |x| format!("{}ns", x)),
        Decimal(_, scale) => {
            // The number 999.99 has a precision of 5 and scale of 2
            let scale = *scale as u32;
            let display = move |x| {
                let base = x / 10i128.pow(scale);
                let decimals = x - base * 10i128.pow(scale);
                format!("{}.{}", base, decimals)
            };
            dyn_primitive!(array, i128, display)
        }
        _ => unreachable!(),
    }
}

impl<T: NativeType> Debug for PrimitiveArray<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let writer = get_write_value(self);

        write!(f, "{:?}", self.data_type())?;
        write_vec(f, &*writer, self.validity(), self.len(), "None", false)
    }
}
