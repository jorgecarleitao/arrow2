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

//! Basic comparison kernels.
//!
//! The module contains functions that compare either an array and a scalar
//! or two arrays of the same [`DataType`]. The scalar-oriented functions are
//! suffixed with `_scalar`. In general, these comparison functions receive as
//! inputs the two items for comparison and an [`Operator`] which specifies the
//! type of comparison that will be conducted, such as `<=` ([`Operator::LtEq`]).
//!
//! Much like the parent module [`compute`](crate::compute), the comparison functions
//! have two variants - a statically typed one ([`primitive_compare`])
//! which expects concrete types such as [`Int8Array`] and a dynamically typed
//! variant ([`compare`]) that compares values of type `&dyn Array` and errors
//! if the underlying concrete types mismsatch. The statically-typed functions
//! are appropriately prefixed with the concrete types they expect.
//!
//! Also note that the scalar-based comparison functions for the concrete types,
//! like [`primitive_compare_scalar`], are infallible and always return a
//! [`BooleanArray`] while the rest of the functions always wrap the returned
//! array in a [`Result`] due to their internal checks of the data types and
//! lengths.
//!
//! # Examples
//!
//! Compare two [`PrimitiveArray`]s:
//! ```
//! use arrow2::compute::comparison::{primitive_compare, Operator};
//! # use arrow2::array::{BooleanArray, PrimitiveArray};
//! # use arrow2::error::{ArrowError, Result};
//!
//! let array1 = PrimitiveArray::<i32>::from([Some(1), None, Some(2)]);
//! let array2 = PrimitiveArray::<i32>::from([Some(1), None, Some(1)]);
//! let result = primitive_compare(&array1, &array2, Operator::Gt)?;
//! assert_eq!(result, BooleanArray::from([Some(false), None, Some(true)]));
//! # Ok::<(), ArrowError>(())
//! ```
//! Compare two dynamically-typed arrays (trait objects):
//! ```
//! use arrow2::compute::comparison::{compare, Operator};
//! # use arrow2::array::{Array, BooleanArray, PrimitiveArray};
//! # use arrow2::error::{ArrowError, Result};
//!
//! let array1: &dyn Array = &PrimitiveArray::<f64>::from(&[Some(10.0), None, Some(20.0)]);
//! let array2: &dyn Array = &PrimitiveArray::<f64>::from(&[Some(10.0), None, Some(10.0)]);
//! let result = compare(array1, array2, Operator::LtEq)?;
//! assert_eq!(result, BooleanArray::from([Some(true), None, Some(false)]));
//! # Ok::<(), ArrowError>(())
//! ```
//! Compare an array of strings to a "scalar", i.e a word (note that we use
//! [`Operator::Neq`]):
//! ```
//! use arrow2::compute::comparison::{utf8_compare_scalar, Operator};
//! # use arrow2::array::{Array, BooleanArray, Utf8Array};
//! # use arrow2::scalar::Utf8Scalar;
//! # use arrow2::error::{ArrowError, Result};
//!
//! let array = Utf8Array::<i32>::from([Some("compute"), None, Some("compare")]);
//! let word = Utf8Scalar::new(Some("compare"));
//! let result = utf8_compare_scalar(&array, &word, Operator::Neq);
//! assert_eq!(result, BooleanArray::from([Some(true), None, Some(false)]));
//! # Ok::<(), ArrowError>(())
//! ```

use crate::array::*;
use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::scalar::Scalar;

mod binary;
mod boolean;
mod primitive;
mod utf8;

mod simd;
pub use simd::{Simd8, Simd8Lanes};

pub use binary::compare as binary_compare;
pub use binary::compare_scalar as binary_compare_scalar;
pub use boolean::compare as boolean_compare;
pub use boolean::compare_scalar as boolean_compare_scalar;
pub use primitive::compare as primitive_compare;
pub use primitive::compare_scalar as primitive_compare_scalar;
pub(crate) use primitive::compare_values_op as primitive_compare_values_op;
pub use utf8::compare as utf8_compare;
pub use utf8::compare_scalar as utf8_compare_scalar;

/// Comparison operators, such as `>` ([`Operator::Gt`])
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Lt,
    LtEq,
    Gt,
    GtEq,
    Eq,
    Neq,
}

/// Compares each slot of `lhs` against each slot of `rhs`.
/// # Error
/// Errors iff:
/// * `lhs.data_type() != rhs.data_type()` or
/// * `lhs.len() != rhs.len()` or
/// * the datatype is not supported (use [`can_compare`] to tell whether it is supported)
pub fn compare(lhs: &dyn Array, rhs: &dyn Array, operator: Operator) -> Result<BooleanArray> {
    let data_type = lhs.data_type();
    if data_type != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Comparison is only supported for arrays of the same logical type".to_string(),
        ));
    }
    match data_type {
        DataType::Boolean => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            boolean::compare(lhs, rhs, operator)
        }
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<i8>(lhs, rhs, operator)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<i16>(lhs, rhs, operator)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<i32>(lhs, rhs, operator)
        }
        DataType::Int64
        | DataType::Timestamp(_, None)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<i64>(lhs, rhs, operator)
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<u8>(lhs, rhs, operator)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<u16>(lhs, rhs, operator)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<u32>(lhs, rhs, operator)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<u64>(lhs, rhs, operator)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<f32>(lhs, rhs, operator)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<f64>(lhs, rhs, operator)
        }
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::compare::<i32>(lhs, rhs, operator)
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::compare::<i64>(lhs, rhs, operator)
        }
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare::<i128>(lhs, rhs, operator)
        }
        DataType::Binary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::compare::<i32>(lhs, rhs, operator)
        }
        DataType::LargeBinary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::compare::<i64>(lhs, rhs, operator)
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Comparison between {:?} is not supported",
            data_type
        ))),
    }
}

/// Compares all slots of `lhs` against `rhs`.
/// # Error
/// Errors iff:
/// * `lhs.data_type() != rhs.data_type()` or
/// * the datatype is not supported (use [`can_compare`] to tell whether it is supported)
pub fn compare_scalar(
    lhs: &dyn Array,
    rhs: &dyn Scalar,
    operator: Operator,
) -> Result<BooleanArray> {
    let data_type = lhs.data_type();
    if data_type != rhs.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "Comparison is only supported for the same logical type".to_string(),
        ));
    }
    Ok(match data_type {
        DataType::Boolean => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            boolean::compare_scalar(lhs, rhs, operator)
        }
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<i8>(lhs, rhs, operator)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<i16>(lhs, rhs, operator)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<i32>(lhs, rhs, operator)
        }
        DataType::Int64
        | DataType::Timestamp(_, None)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<i64>(lhs, rhs, operator)
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<u8>(lhs, rhs, operator)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<u16>(lhs, rhs, operator)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<u32>(lhs, rhs, operator)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<u64>(lhs, rhs, operator)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<f32>(lhs, rhs, operator)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<f64>(lhs, rhs, operator)
        }
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::compare_scalar::<i128>(lhs, rhs, operator)
        }
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::compare_scalar::<i32>(lhs, rhs, operator)
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::compare_scalar::<i64>(lhs, rhs, operator)
        }
        DataType::Binary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::compare_scalar::<i32>(lhs, rhs, operator)
        }
        DataType::LargeBinary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::compare_scalar::<i64>(lhs, rhs, operator)
        }
        _ => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Comparison between {:?} is not supported",
                data_type
            )))
        }
    })
}
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
/// assert_eq!(can_compare(&data_type), true)
/// ```
pub fn can_compare(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
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
            | DataType::Binary
            | DataType::LargeBinary
    )
}
