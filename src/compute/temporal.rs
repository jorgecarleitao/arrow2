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

/// Extracts the years of a temporal array as [`PrimitiveArray<i32>`].
/// Use [`can_year`] to check if this operation is supported for the target [`DataType`].
pub fn year(array: &dyn Array) -> Result<PrimitiveArray<i32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            date_variants(array, DataType::Int32, |x| x.year())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.year()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.year())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"year\" does not support type {:?}",
            dt
        ))),
    }
}

/// Extracts the months of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_month`] to check if this operation is supported for the target [`DataType`].
pub fn month(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            date_variants(array, DataType::UInt32, |x| x.month())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.month()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.month())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"month\" does not support type {:?}",
            dt
        ))),
    }
}

/// Extracts the days of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_day`] to check if this operation is supported for the target [`DataType`].
pub fn day(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            date_variants(array, DataType::UInt32, |x| x.day())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.day()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.day())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"day\" does not support type {:?}",
            dt
        ))),
    }
}

/// Extracts the hours of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_hour`] to check if this operation is supported for the target [`DataType`].
pub fn hour(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | &DataType::Timestamp(_, None) => {
            date_variants(array, DataType::UInt32, |x| x.hour())
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            time_variants(array, DataType::UInt32, |x| x.hour())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.hour()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.hour())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"hour\" does not support type {:?}",
            dt
        ))),
    }
}

/// Extracts the minutes of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_minute`] to check if this operation is supported for the target [`DataType`].
pub fn minute(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | &DataType::Timestamp(_, None) => {
            date_variants(array, DataType::UInt32, |x| x.minute())
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            time_variants(array, DataType::UInt32, |x| x.minute())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.minute()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.minute())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"minute\" does not support type {:?}",
            dt
        ))),
    }
}

/// Extracts the seconds of a temporal array as [`PrimitiveArray<u32>`].
/// Use [`can_second`] to check if this operation is supported for the target [`DataType`].
pub fn second(array: &dyn Array) -> Result<PrimitiveArray<u32>> {
    match array.data_type() {
        DataType::Date32 | DataType::Date64 | &DataType::Timestamp(_, None) => {
            date_variants(array, DataType::UInt32, |x| x.second())
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            time_variants(array, DataType::UInt32, |x| x.second())
        }
        DataType::Timestamp(time_unit, Some(timezone_str)) => {
            let time_unit = *time_unit;
            let timezone = parse_offset(timezone_str);

            let array = array.as_any().downcast_ref().unwrap();

            if let Ok(timezone) = timezone {
                Ok(extract_impl(array, time_unit, timezone, |x| x.second()))
            } else {
                chrono_tz(array, time_unit, timezone_str, |x| x.second())
            }
        }
        dt => Err(ArrowError::NotYetImplemented(format!(
            "\"second\" does not support type {:?}",
            dt
        ))),
    }
}

pub fn date_variants<F, O>(
    array: &dyn Array,
    data_type: DataType,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: NativeType,
    F: Fn(chrono::NaiveDateTime) -> O,
{
    match array.data_type() {
        DataType::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(array, |x| op(date32_to_datetime(x)), data_type))
        }
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(array, |x| op(date64_to_datetime(x)), data_type))
        }
        DataType::Timestamp(time_unit, None) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            let func = match time_unit {
                TimeUnit::Second => timestamp_s_to_datetime,
                TimeUnit::Millisecond => timestamp_ms_to_datetime,
                TimeUnit::Microsecond => timestamp_us_to_datetime,
                TimeUnit::Nanosecond => timestamp_ns_to_datetime,
            };
            Ok(unary(array, |x| op(func(x)), data_type))
        }
        _ => unreachable!(),
    }
}

fn time_variants<F, O>(array: &dyn Array, data_type: DataType, op: F) -> Result<PrimitiveArray<O>>
where
    O: NativeType,
    F: Fn(chrono::NaiveTime) -> O,
{
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(array, |x| op(time32s_to_time(x)), data_type))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Ok(unary(array, |x| op(time32ms_to_time(x)), data_type))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(array, |x| op(time64us_to_time(x)), data_type))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Ok(unary(array, |x| op(time64ns_to_time(x)), data_type))
        }
        _ => unreachable!(),
    }
}

#[cfg(feature = "chrono-tz")]
#[cfg_attr(docsrs, doc(cfg(feature = "chrono-tz")))]
fn chrono_tz<F, O>(
    array: &PrimitiveArray<i64>,
    time_unit: TimeUnit,
    timezone_str: &str,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    O: NativeType,
    F: Fn(chrono::DateTime<chrono_tz::Tz>) -> O,
{
    let timezone = parse_offset_tz(timezone_str)?;
    Ok(extract_impl(array, time_unit, timezone, op))
}

#[cfg(not(feature = "chrono-tz"))]
fn chrono_tz<F, O>(
    _: &PrimitiveArray<i64>,
    _: TimeUnit,
    timezone_str: &str,
    _: F,
) -> Result<PrimitiveArray<O>>
where
    O: NativeType,
    F: Fn(chrono::DateTime<chrono_tz::Tz>) -> O,
{
    Err(ArrowError::InvalidArgumentError(format!(
        "timezone \"{}\" cannot be parsed (feature chrono-tz is not active)",
        timezone_str
    )))
}

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
    can_date(data_type)
}

/// Checks if an array of type `datatype` can perform month operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_month;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Date32;
/// assert_eq!(can_month(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_month(&data_type), false);
/// ```
pub fn can_month(data_type: &DataType) -> bool {
    can_date(data_type)
}

/// Checks if an array of type `datatype` can perform day operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_day;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Date32;
/// assert_eq!(can_day(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_day(&data_type), false);
/// ```
pub fn can_day(data_type: &DataType) -> bool {
    can_date(data_type)
}

fn can_date(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
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
    can_time(data_type)
}

/// Checks if an array of type `datatype` can perform minute operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_minute;
/// use arrow2::datatypes::{DataType, TimeUnit};
///
/// let data_type = DataType::Time32(TimeUnit::Second);
/// assert_eq!(can_minute(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_minute(&data_type), false);
/// ```
pub fn can_minute(data_type: &DataType) -> bool {
    can_time(data_type)
}

/// Checks if an array of type `datatype` can perform second operation
///
/// # Examples
/// ```
/// use arrow2::compute::temporal::can_second;
/// use arrow2::datatypes::{DataType, TimeUnit};
///
/// let data_type = DataType::Time32(TimeUnit::Second);
/// assert_eq!(can_second(&data_type), true);

/// let data_type = DataType::Int8;
/// assert_eq!(can_second(&data_type), false);
/// ```
pub fn can_second(data_type: &DataType) -> bool {
    can_time(data_type)
}

fn can_time(data_type: &DataType) -> bool {
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
