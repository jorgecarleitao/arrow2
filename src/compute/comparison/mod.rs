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

//! Defines basic comparison kernels for [`PrimitiveArray`]s.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use crate::array::*;
use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::types::days_ms;

mod primitive;
mod utf8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Lt,
    LtEq,
    Gt,
    GtEq,
    Eq,
    Neq,
}

pub fn compare(lhs: &dyn Array, rhs: &dyn Array, operator: Operator) -> Result<BooleanArray> {
    let data_type = lhs.data_type();
    if data_type != rhs.data_type() {
        return Err(ArrowError::NotYetImplemented(
            "Comparison is only supported for arrays of the same logical type".to_string(),
        ));
    }
    match data_type {
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref::<Int8Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int8Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref::<Int16Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int16Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref::<Int32Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int32Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Int64
        | DataType::Timestamp(_, None)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref::<Int64Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int64Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref::<UInt8Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<UInt8Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref::<UInt16Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<UInt16Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref::<UInt32Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<UInt32Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref::<UInt64Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<UInt64Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref::<Float32Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Float32Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref::<Float64Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Float64Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let lhs = lhs
                .as_any()
                .downcast_ref::<PrimitiveArray<days_ms>>()
                .unwrap();
            let rhs = rhs
                .as_any()
                .downcast_ref::<PrimitiveArray<days_ms>>()
                .unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            utf8::compare(lhs, rhs, operator)
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            utf8::compare(lhs, rhs, operator)
        }
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref::<Int128Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int128Array>().unwrap();
            primitive::compare(lhs, rhs, operator)
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Comparison between {:?} is not supported",
            data_type
        ))),
    }
}

pub use primitive::compare_scalar as primitive_compare_scalar;
pub(crate) use primitive::compare_values_op as primitive_compare_values_op;
pub use utf8::compare_scalar as utf8_compare_scalar;

/// Checks if an array of type `datatype` can be compared with another array of
/// the same type.
///
/// # Examples
/// ```
/// use arrow2::compute::comparison::can_compare;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Int8;
/// assert_eq!(can_compare(&data_type), true);
///
/// let data_type = DataType::LargeBinary;
/// assert_eq!(can_compare(&data_type), false)
/// ```
pub fn can_compare(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(_)
            | DataType::Int64
            | DataType::Timestamp(_, None)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency() {
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

        datatypes.into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_compare(&d1) {
                let op = Operator::Eq;
                assert!(compare(array.as_ref(), array.as_ref(), op).is_ok());
            } else {
                let op = Operator::Eq;
                assert!(compare(array.as_ref(), array.as_ref(), op).is_err());
            }
        });
    }
}
