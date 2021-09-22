// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines temporal kernels for time and date related functions.

use chrono::{Datelike, Timelike};

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::temporal_conversions::*;
use crate::types::NativeType;
use crate::types::NaturalDataType;

use super::arity::unary;

fn extract_impl<T, A, F>(
    array: &PrimitiveArray<i64>,
    time_unit: TimeUnit,
    timezone: T,
    extract: F,
) -> PrimitiveArray<A>
where
    T: chrono::TimeZone,
    A: NativeType + NaturalDataType,
    F: Fn(chrono::DateTime<T>) -> A,
{
    match time_unit {
        TimeUnit::Second => {
            let op = |x| {
                let datetime = timestamp_s_to_datetime(x);
                let offset = timezone.offset_from_utc_datetime(&datetime);
                extract(chrono::DateTime::<T>::from_utc(datetime, offset))
            };
            unary(array, op, DataType::UInt32)
        }
        TimeUnit::Millisecond => {
            let op = |x| {
                let datetime = timestamp_ms_to_datetime(x);
                let offset = timezone.offset_from_utc_datetime(&datetime);
                extract(chrono::DateTime::<T>::from_utc(datetime, offset))
            };
            unary(array, op, A::DATA_TYPE)
        }
        TimeUnit::Microsecond => {
            let op = |x| {
                let datetime = timestamp_us_to_datetime(x);
                let offset = timezone.offset_from_utc_datetime(&datetime);
                extract(chrono::DateTime::<T>::from_utc(datetime, offset))
            };
            unary(array, op, A::DATA_TYPE)
        }
        TimeUnit::Nanosecond => {
            let op = |x| {
                let datetime = timestamp_ns_to_datetime(x);
                let offset = timezone.offset_from_utc_datetime(&datetime);
                extract(chrono::DateTime::<T>::from_utc(datetime, offset))
            };
            unary(array, op, A::DATA_TYPE)
        }
    }
}

#[cfg(feature = "chrono-tz")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono-tz")))]
fn chrono_tz_hour(
    array: &PrimitiveArray<i64>,
    time_unit: TimeUnit,
    timezone_str: &str,
) -> Result<PrimitiveArray<u32>> {
    let timezone = parse_offset_tz(timezone_str)?;
    Ok(extract_impl(array, time_unit, timezone, |x| x.hour()))
}

#[cfg(not(feature = "chrono-tz"))]
fn chrono_tz_hour(
    _: &PrimitiveArray<i64>,
    _: TimeUnit,
    timezone_str: &str,
) -> Result<PrimitiveArray<u32>> {
    Err(ArrowError::InvalidArgumentError(format!(
        "timezone \"{}\" cannot be parsed (feature chrono-tz is not active)",
        timezone_str
    )))
}

#[cfg(feature = "chrono-tz")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono-tz")))]
fn chrono_tz_year(
    array: &PrimitiveArray<i64>,
    time_unit: TimeUnit,
    timezone_str: &str,
) -> Result<PrimitiveArray<i32>> {
    let timezone = parse_offset_tz(timezone_str)?;
    Ok(extract_impl(array, time_unit, timezone, |x| x.year()))
}

#[cfg(not(feature = "chrono-tz"))]
fn chrono_tz_year(
    _: &PrimitiveArray<i64>,
    _: TimeUnit,
    timezone_str: &str,
) -> Result<PrimitiveArray<i32>> {
    Err(ArrowError::InvalidArgumentError(format!(
        "timezone \"{}\" cannot be parsed (feature chrono-tz is not active)",
        timezone_str
    )))
}

/// Extracts the hours of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_hour`] to check if this operation is supported for the target [`DataType`].
pub fn hour(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    let final_data_type = DataType::UInt32;
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(array, |x| time32s_to_time(x).hour(), final_data_type))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(
                array,
                |x| time32ms_to_time(x).hour(),
                final_data_type,
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(
                array,
                |x| time64us_to_time(x).hour(),
                final_data_type,
            ))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(
                array,
                |x| time64ns_to_time(x).hour(),
                final_data_type,
            ))
        }
        DataType::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(
                array,
                |x| date32_to_datetime(x).hour(),
                final_data_type,
            ))
        }
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(
                array,
                |x| date64_to_datetime(x).hour(),
                final_data_type,
            ))
        }
        DataType::Timestamp(time_unit, None) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            let op = match time_unit {
                TimeUnit::Second => |x| timestamp_s_to_datetime(x).hour(),
                TimeUnit::Millisecond => |x| timestamp_ms_to_datetime(x).hour(),
                TimeUnit::Microsecond => |x| timestamp_us_to_datetime(x).hour(),
                TimeUnit::Nanosecond => |x| timestamp_ns_to_datetime(x).hour(),
            };
            Ok(unary(array, op, final_data_type))
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.hour()))
            } else {
                chrono_tz_hour(array, time_unit, timezone_str)
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"hour\" does not support type {:?}",
            dt
        ))),
    }
}

/// Checks if an array of type `datatype` can perform hour operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_hour;
/// use arrow2::datatypes::{DataType, TimeUnit};
///
/// let data_type = DataType::Time32(TimeUnit::Second);
/// assert_eq!(can_hour(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_hour(&data_type), false);
/// ```
pub fn can_hour(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

/// Extracts the years of a temporal array as [`PrimitiveArray<i32>`].
/// Use [`can_year`] to check if this operation is supported for the target [`DataType`].
pub fn year(array: &dyn Array) -> Result<PrimitiveArray<i32>> {
    let final_data_type = DataType::Int32;
    match array.data_type() {
        DataType::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(
                array,
                |x| date32_to_datetime(x).year(),
                final_data_type,
            ))
        }
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(
                array,
                |x| date64_to_datetime(x).year(),
                final_data_type,
            ))
        }
        DataType::Timestamp(time_unit, None) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            let op = match time_unit {
                TimeUnit::Second => |x| timestamp_s_to_datetime(x).year(),
                TimeUnit::Millisecond => |x| timestamp_ms_to_datetime(x).year(),
                TimeUnit::Microsecond => |x| timestamp_us_to_datetime(x).year(),
                TimeUnit::Nanosecond => |x| timestamp_ns_to_datetime(x).year(),
            };
            Ok(unary(array, op, final_data_type))
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.year()))
            } else {
                chrono_tz_year(array, time_unit, timezone_str)
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"year\" does not support type {:?}",
            dt
        ))),
    }
}

/// Checks if an array of type `datatype` can perform year operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_year;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Date32;
/// assert_eq!(can_year(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_year(&data_type), false);
/// ```
pub fn can_year(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}
