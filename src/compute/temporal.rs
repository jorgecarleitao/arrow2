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

use super::arity::unary;

/// Extracts the hours of a given temporal array as an array of integers
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
        DataType::Time32(TimeUnit::Microsecond) => {
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
    match data_type {
        DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, None) => true,
        _ => false,
    }
}

/// Extracts the hours of a given temporal array as an array of integers
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
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => true,
        _ => false,
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date64_hour() {
        let array = Primitive::<i64>::from(&[Some(1514764800000), None, Some(1550636625000)])
            .to(DataType::Date64);

        let result = hour(&array).unwrap();
        let expected = UInt32Array::from(&[Some(0), None, Some(4)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn date32_hour() {
        let array = Primitive::<i32>::from(&[Some(15147), None, Some(15148)]).to(DataType::Date32);

        let result = hour(&array).unwrap();
        let expected = UInt32Array::from(&[Some(0), None, Some(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn time32_second_hour() {
        let array =
            Primitive::<i32>::from(&[Some(37800), None]).to(DataType::Time32(TimeUnit::Second));

        let result = hour(&array).unwrap();
        let expected = UInt32Array::from(&[Some(10), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn time64_micro_hour() {
        let array = Primitive::<i64>::from(&[Some(37800000000), None])
            .to(DataType::Time64(TimeUnit::Microsecond));

        let result = hour(&array).unwrap();
        let expected = UInt32Array::from(&[Some(10), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn timestamp_micro_hour() {
        let array = Primitive::<i64>::from(&[Some(37800000000), None])
            .to(DataType::Timestamp(TimeUnit::Microsecond, None));

        let result = hour(&array).unwrap();
        let expected = UInt32Array::from(&[Some(10), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn timestamp_date64_year() {
        let array = Primitive::<i64>::from(&[Some(1514764800000), None]).to(DataType::Date64);

        let result = year(&array).unwrap();
        let expected = Int32Array::from(&[Some(2018), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn timestamp_date32_year() {
        let array = Primitive::<i32>::from(&[Some(15147), None]).to(DataType::Date32);

        let result = year(&array).unwrap();
        let expected = Int32Array::from(&[Some(2011), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn timestamp_micro_year() {
        let array = Primitive::<i64>::from(&[Some(1612025847000000), None])
            .to(DataType::Timestamp(TimeUnit::Microsecond, None));

        let result = year(&array).unwrap();
        let expected = Int32Array::from(&[Some(2021), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn consistency_hour() {
        use crate::array::new_null_array;
        use crate::datatypes::DataType::*;
        use crate::datatypes::TimeUnit;

        let datatypes = vec![
            Null,
            Boolean,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Int8,
            Int16,
            Int32,
            Int64,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Date32,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Date64,
            Utf8,
            LargeUtf8,
            Binary,
            LargeBinary,
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
        ];

        datatypes.clone().into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_hour(&d1) {
                assert!(hour(array.as_ref()).is_ok());
            } else {
                assert!(hour(array.as_ref()).is_err());
            }
        });
    }

    #[test]
    fn consistency_year() {
        use crate::array::new_null_array;
        use crate::datatypes::DataType::*;
        use crate::datatypes::TimeUnit;

        let datatypes = vec![
            Null,
            Boolean,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Int8,
            Int16,
            Int32,
            Int64,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Date32,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Date64,
            Utf8,
            LargeUtf8,
            Binary,
            LargeBinary,
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
        ];

        datatypes.clone().into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_year(&d1) {
                assert!(year(array.as_ref()).is_ok());
            } else {
                assert!(year(array.as_ref()).is_err());
            }
        });
    }
}
