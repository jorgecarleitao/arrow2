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

//! Defines basic arithmetic kernels for [`PrimitiveArray`]s.
//!
//! # Description
//!
//! The Arithmetics module is composed by basic arithmetics operations that can
//! be performed on PrimitiveArray Arrays. These operations can be the building for
//! any implementation using Arrow.
//!
//! Whenever possible, each of the operations in these modules has variations
//! of the basic operation that offers different guarantees. These options are:
//!
//! * plain: The plain type (add, sub, mul, and div) don't offer any protection
//!   when performing the operations. This means that if overflow is found,
//!   then the operations will panic.
//!
//! * checked: A checked operation will change the validity Bitmap for the
//!   offending operation. For example, if one of the operations overflows, the
//!   validity will be changed to None, indicating a Null value.
//!
//! * saturating: If overflowing is presented in one operation, the resulting
//!   value for that index will be saturated to the MAX or MIN value possible
//!   for that type. For [`Decimal`](crate::datatypes::DataType::Decimal)
//!   arrays, the saturated value is calculated considering the precision and
//!   scale of the array.
//!
//! * overflowing: When an operation overflows, the resulting will be the
//!   overflowed value for the operation. The result from the array operation
//!   includes a Binary bitmap indicating which values overflowed.
//!
//! * adaptive: For [`Decimal`](crate::datatypes::DataType::Decimal) arrays,
//!   the adaptive variation adjusts the precision and scale to avoid
//!   saturation or overflowing.
//!
//! # New kernels
//!
//! When adding a new operation to this module, it is strongly suggested to
//! follow the design description presented in the README.md file located in
//! the [`compute`](crate::compute) module and the function descriptions
//! presented in this document.

pub mod basic;
pub mod decimal;
pub mod time;

use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use num_traits::{CheckedNeg, NumCast, WrappingNeg, Zero};

use crate::datatypes::{DataType, IntervalUnit, TimeUnit};
use crate::error::{ArrowError, Result};
use crate::types::NativeType;
use crate::{array::*, bitmap::Bitmap};

use super::arity::{unary, unary_checked};

// Macro to evaluate match branch in arithmetic function.
// The macro is used to downcast both arrays to a primitive_array_type. If there
// is an error then an ArrowError is return with the data_type that cause it.
// It returns the result from the arithmetic_primitive function evaluated with
// the Operator selected
macro_rules! primitive {
    ($lhs: expr, $rhs: expr, $op: expr, $array_type: ty) => {{
        let res_lhs = $lhs.as_any().downcast_ref().unwrap();
        let res_rhs = $rhs.as_any().downcast_ref().unwrap();

        let res = arithmetic_primitive::<$array_type>(res_lhs, $op, res_rhs);
        Ok(Box::new(res) as Box<dyn Array>)
    }};
}

/// Execute an arithmetic operation with two arrays. It uses the enum Operator
/// to select the type of operation that is going to be performed with the two
/// arrays
pub fn arithmetic(lhs: &dyn Array, op: Operator, rhs: &dyn Array) -> Result<Box<dyn Array>> {
    use DataType::*;
    use Operator::*;
    match (lhs.data_type(), op, rhs.data_type()) {
        (Int8, _, Int8) => primitive!(lhs, rhs, op, i8),
        (Int16, _, Int16) => primitive!(lhs, rhs, op, i16),
        (Int32, _, Int32) => primitive!(lhs, rhs, op, i32),
        (Int64, _, Int64) | (Duration(_), _, Duration(_)) => {
            primitive!(lhs, rhs, op, i64)
        }
        (UInt8, _, UInt8) => primitive!(lhs, rhs, op, u8),
        (UInt16, _, UInt16) => primitive!(lhs, rhs, op, u16),
        (UInt32, _, UInt32) => primitive!(lhs, rhs, op, u32),
        (UInt64, _, UInt64) => primitive!(lhs, rhs, op, u64),
        (Float32, _, Float32) => primitive!(lhs, rhs, op, f32),
        (Float64, _, Float64) => primitive!(lhs, rhs, op, f64),
        (Decimal(_, _), _, Decimal(_, _)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();

            let res = match op {
                Add => decimal::add::add(lhs, rhs),
                Subtract => decimal::sub::sub(lhs, rhs),
                Multiply => decimal::mul::mul(lhs, rhs),
                Divide => decimal::div::div(lhs, rhs),
                Remainder => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Arithmetics of ({:?}, {:?}, {:?}) is not supported",
                        lhs, op, rhs
                    )))
                }
            };

            Ok(Box::new(res) as Box<dyn Array>)
        }
        (Time32(TimeUnit::Second), Add, Duration(_))
        | (Time32(TimeUnit::Millisecond), Add, Duration(_))
        | (Date32, Add, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::add_duration::<i32>(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time32(TimeUnit::Second), Subtract, Duration(_))
        | (Time32(TimeUnit::Millisecond), Subtract, Duration(_))
        | (Date32, Subtract, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::subtract_duration::<i32>(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time64(TimeUnit::Microsecond), Add, Duration(_))
        | (Time64(TimeUnit::Nanosecond), Add, Duration(_))
        | (Date64, Add, Duration(_))
        | (Timestamp(_, _), Add, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::add_duration::<i64>(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Timestamp(_, _), Add, Interval(IntervalUnit::MonthDayNano)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::add_interval(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Time64(TimeUnit::Microsecond), Subtract, Duration(_))
        | (Time64(TimeUnit::Nanosecond), Subtract, Duration(_))
        | (Date64, Subtract, Duration(_))
        | (Timestamp(_, _), Subtract, Duration(_)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::subtract_duration::<i64>(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (Timestamp(_, None), Subtract, Timestamp(_, None)) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            time::subtract_timestamps(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>)
        }
        (lhs, op, rhs) => Err(ArrowError::NotYetImplemented(format!(
            "Arithmetics of ({:?}, {:?}, {:?}) is not supported",
            lhs, op, rhs
        ))),
    }
}

/// Checks if an array of type `datatype` can perform basic arithmetic
/// operations. These operations include add, subtract, multiply, divide.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::{can_arithmetic, Operator};
/// use arrow2::datatypes::DataType;
///
/// let data_type = DataType::Int8;
/// assert_eq!(can_arithmetic(&data_type, Operator::Add, &data_type), true);
///
/// let data_type = DataType::LargeBinary;
/// assert_eq!(can_arithmetic(&data_type, Operator::Add, &data_type), false)
/// ```
pub fn can_arithmetic(lhs: &DataType, op: Operator, rhs: &DataType) -> bool {
    use DataType::*;
    use Operator::*;
    if let (Decimal(_, _), Remainder, Decimal(_, _)) = (lhs, op, rhs) {
        return false;
    };

    matches!(
        (lhs, op, rhs),
        (Int8, _, Int8)
            | (Int16, _, Int16)
            | (Int32, _, Int32)
            | (Int64, _, Int64)
            | (UInt8, _, UInt8)
            | (UInt16, _, UInt16)
            | (UInt32, _, UInt32)
            | (UInt64, _, UInt64)
            | (Float64, _, Float64)
            | (Float32, _, Float32)
            | (Duration(_), _, Duration(_))
            | (Decimal(_, _), _, Decimal(_, _))
            | (Date32, Subtract, Duration(_))
            | (Date32, Add, Duration(_))
            | (Date64, Subtract, Duration(_))
            | (Date64, Add, Duration(_))
            | (Time32(TimeUnit::Millisecond), Subtract, Duration(_))
            | (Time32(TimeUnit::Second), Subtract, Duration(_))
            | (Time32(TimeUnit::Millisecond), Add, Duration(_))
            | (Time32(TimeUnit::Second), Add, Duration(_))
            | (Time64(TimeUnit::Microsecond), Subtract, Duration(_))
            | (Time64(TimeUnit::Nanosecond), Subtract, Duration(_))
            | (Time64(TimeUnit::Microsecond), Add, Duration(_))
            | (Time64(TimeUnit::Nanosecond), Add, Duration(_))
            | (Timestamp(_, _), Subtract, Duration(_))
            | (Timestamp(_, _), Add, Duration(_))
            | (Timestamp(_, _), Add, Interval(IntervalUnit::MonthDayNano))
            | (Timestamp(_, None), Subtract, Timestamp(_, None))
    )
}

/// Arithmetic operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    /// Add
    Add,
    /// Subtract
    Subtract,
    /// Multiply
    Multiply,
    /// Divide
    Divide,
    /// Remainder
    Remainder,
}

/// Perform arithmetic operations on two primitive arrays based on the Operator enum
//
pub fn arithmetic_primitive<T>(
    lhs: &PrimitiveArray<T>,
    op: Operator,
    rhs: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    T: NativeType
        + Div<Output = T>
        + Zero
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Rem<Output = T>,
{
    match op {
        Operator::Add => basic::add(lhs, rhs),
        Operator::Subtract => basic::sub(lhs, rhs),
        Operator::Multiply => basic::mul(lhs, rhs),
        Operator::Divide => basic::div(lhs, rhs),
        Operator::Remainder => basic::rem(lhs, rhs),
    }
}

/// Performs primitive operation on an array and and scalar
pub fn arithmetic_primitive_scalar<T>(
    lhs: &PrimitiveArray<T>,
    op: Operator,
    rhs: &T,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType
        + Div<Output = T>
        + Zero
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Rem<Output = T>
        + NumCast,
{
    match op {
        Operator::Add => Ok(basic::add_scalar(lhs, rhs)),
        Operator::Subtract => Ok(basic::sub_scalar(lhs, rhs)),
        Operator::Multiply => Ok(basic::mul_scalar(lhs, rhs)),
        Operator::Divide => Ok(basic::div_scalar(lhs, rhs)),
        Operator::Remainder => Ok(basic::rem_scalar(lhs, rhs)),
    }
}

/// Negates values from array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::negate;
/// use arrow2::array::PrimitiveArray;
///
/// let a = PrimitiveArray::from([None, Some(6), None, Some(7)]);
/// let result = negate(&a);
/// let expected = PrimitiveArray::from([None, Some(-6), None, Some(-7)]);
/// assert_eq!(result, expected)
/// ```
pub fn negate<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + Neg<Output = T>,
{
    unary(array, |a| -a, array.data_type().clone())
}

/// Checked negates values from array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::checked_negate;
/// use arrow2::array::{Array, PrimitiveArray};
///
/// let a = PrimitiveArray::from([None, Some(6), Some(i8::MIN), Some(7)]);
/// let result = checked_negate(&a);
/// let expected = PrimitiveArray::from([None, Some(-6), None, Some(-7)]);
/// assert_eq!(result, expected);
/// assert!(!result.is_valid(2))
/// ```
pub fn checked_negate<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + CheckedNeg,
{
    unary_checked(array, |a| a.checked_neg(), array.data_type().clone())
}

/// Wrapping negates values from array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::wrapping_negate;
/// use arrow2::array::{Array, PrimitiveArray};
///
/// let a = PrimitiveArray::from([None, Some(6), Some(i8::MIN), Some(7)]);
/// let result = wrapping_negate(&a);
/// let expected = PrimitiveArray::from([None, Some(-6), Some(i8::MIN), Some(-7)]);
/// assert_eq!(result, expected);
/// ```
pub fn wrapping_negate<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + WrappingNeg,
{
    unary(array, |a| a.wrapping_neg(), array.data_type().clone())
}

/// Defines basic addition operation for primitive arrays
pub trait ArrayAdd<Rhs>: Sized {
    /// Adds itself to `rhs`
    fn add(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping addition operation for primitive arrays
pub trait ArrayWrappingAdd<Rhs>: Sized {
    /// Adds itself to `rhs` using wrapping addition
    fn wrapping_add(&self, rhs: &Rhs) -> Self;
}

/// Defines checked addition operation for primitive arrays
pub trait ArrayCheckedAdd<Rhs>: Sized {
    /// Checked add
    fn checked_add(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating addition operation for primitive arrays
pub trait ArraySaturatingAdd<Rhs>: Sized {
    /// Saturating add
    fn saturating_add(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing addition operation for primitive arrays
pub trait ArrayOverflowingAdd<Rhs>: Sized {
    /// Overflowing add
    fn overflowing_add(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic subtraction operation for primitive arrays
pub trait ArraySub<Rhs>: Sized {
    /// subtraction
    fn sub(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping subtraction operation for primitive arrays
pub trait ArrayWrappingSub<Rhs>: Sized {
    /// wrapping subtraction
    fn wrapping_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines checked subtraction operation for primitive arrays
pub trait ArrayCheckedSub<Rhs>: Sized {
    /// checked subtraction
    fn checked_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating subtraction operation for primitive arrays
pub trait ArraySaturatingSub<Rhs>: Sized {
    /// saturarting subtraction
    fn saturating_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing subtraction operation for primitive arrays
pub trait ArrayOverflowingSub<Rhs>: Sized {
    /// overflowing subtraction
    fn overflowing_sub(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic multiplication operation for primitive arrays
pub trait ArrayMul<Rhs>: Sized {
    /// multiplication
    fn mul(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping multiplication operation for primitive arrays
pub trait ArrayWrappingMul<Rhs>: Sized {
    /// wrapping multiplication
    fn wrapping_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines checked multiplication operation for primitive arrays
pub trait ArrayCheckedMul<Rhs>: Sized {
    /// checked multiplication
    fn checked_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating multiplication operation for primitive arrays
pub trait ArraySaturatingMul<Rhs>: Sized {
    /// saturating multiplication
    fn saturating_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing multiplication operation for primitive arrays
pub trait ArrayOverflowingMul<Rhs>: Sized {
    /// overflowing multiplication
    fn overflowing_mul(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic division operation for primitive arrays
pub trait ArrayDiv<Rhs>: Sized {
    /// division
    fn div(&self, rhs: &Rhs) -> Self;
}

/// Defines checked division operation for primitive arrays
pub trait ArrayCheckedDiv<Rhs>: Sized {
    /// checked division
    fn checked_div(&self, rhs: &Rhs) -> Self;
}

/// Defines basic reminder operation for primitive arrays
pub trait ArrayRem<Rhs>: Sized {
    /// remainder
    fn rem(&self, rhs: &Rhs) -> Self;
}

/// Defines checked reminder operation for primitive arrays
pub trait ArrayCheckedRem<Rhs>: Sized {
    /// checked remainder
    fn checked_rem(&self, rhs: &Rhs) -> Self;
}

/// Trait describing a [`NativeType`] whose semantics of arithmetic in Arrow equals
/// the semantics in Rust.
/// A counter example is `i128`, that in arrow represents a decimal while in rust represents
/// a signed integer.
pub trait NativeArithmetics: NativeType {}
impl NativeArithmetics for u8 {}
impl NativeArithmetics for u16 {}
impl NativeArithmetics for u32 {}
impl NativeArithmetics for u64 {}
impl NativeArithmetics for i8 {}
impl NativeArithmetics for i16 {}
impl NativeArithmetics for i32 {}
impl NativeArithmetics for i64 {}
impl NativeArithmetics for f32 {}
impl NativeArithmetics for f64 {}
