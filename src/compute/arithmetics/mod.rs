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

use num::Zero;

use crate::datatypes::{DataType, TimeUnit};
use crate::error::{ArrowError, Result};
use crate::types::NativeType;
use crate::{array::*, bitmap::Bitmap};

use super::arity::unary;

// Macro to evaluate match branch in arithmetic function.
// The macro is used to downcast both arrays to a primitive_array_type. If there
// is an error then an ArrowError is return with the data_type that cause it.
// It returns the result from the arithmetic_primitive function evaluated with
// the Operator selected
macro_rules! primitive {
    ($lhs: expr, $rhs: expr, $op: expr, $array_type: ty) => {{
        let res_lhs = $lhs.as_any().downcast_ref().unwrap();
        let res_rhs = $rhs.as_any().downcast_ref().unwrap();
        arithmetic_primitive::<$array_type>(res_lhs, $op, res_rhs)
            .map(Box::new)
            .map(|x| x as Box<dyn Array>)
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

            res.map(|x| Box::new(x) as Box<dyn Array>)
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
            | (Timestamp(_, None), Subtract, Timestamp(_, None))
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}

/// Perform arithmetic operations on two primitive arrays based on the Operator enum
fn arithmetic_primitive<T>(
    lhs: &PrimitiveArray<T>,
    op: Operator,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
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
        Operator::Add => basic::add::add(lhs, rhs),
        Operator::Subtract => basic::sub::sub(lhs, rhs),
        Operator::Multiply => basic::mul::mul(lhs, rhs),
        Operator::Divide => basic::div::div(lhs, rhs),
        Operator::Remainder => basic::rem::rem(lhs, rhs),
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
        + Rem<Output = T>,
{
    match op {
        Operator::Add => Ok(basic::add::add_scalar(lhs, rhs)),
        Operator::Subtract => Ok(basic::sub::sub_scalar(lhs, rhs)),
        Operator::Multiply => Ok(basic::mul::mul_scalar(lhs, rhs)),
        Operator::Divide => Ok(basic::div::div_scalar(lhs, rhs)),
        Operator::Remainder => Ok(basic::rem::rem_scalar(lhs, rhs)),
    }
}

/// Negates values from array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::negate;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::datatypes::DataType;
///
/// let a = PrimitiveArray::from(&vec![None, Some(6), None, Some(7)]).to(DataType::Int32);
/// let result = negate(&a);
/// let expected = PrimitiveArray::from(&vec![None, Some(-6), None, Some(-7)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
pub fn negate<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + Neg<Output = T>,
{
    unary(array, |a| -a, array.data_type().clone())
}

/// Defines basic addition operation for primitive arrays
pub trait ArrayAdd<Rhs> {
    type Output;

    fn add(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines checked addition operation for primitive arrays
pub trait ArrayCheckedAdd<Rhs> {
    type Output;

    fn checked_add(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines saturating addition operation for primitive arrays
pub trait ArraySaturatingAdd<Rhs> {
    type Output;

    fn saturating_add(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines Overflowing addition operation for primitive arrays
pub trait ArrayOverflowingAdd<Rhs> {
    type Output;

    fn overflowing_add(&self, rhs: &Rhs) -> Result<(Self::Output, Bitmap)>;
}

/// Defines basic subtraction operation for primitive arrays
pub trait ArraySub<Rhs> {
    type Output;

    fn sub(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines checked subtraction operation for primitive arrays
pub trait ArrayCheckedSub<Rhs> {
    type Output;

    fn checked_sub(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines saturating subtraction operation for primitive arrays
pub trait ArraySaturatingSub<Rhs> {
    type Output;

    fn saturating_sub(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines Overflowing subtraction operation for primitive arrays
pub trait ArrayOverflowingSub<Rhs> {
    type Output;

    fn overflowing_sub(&self, rhs: &Rhs) -> Result<(Self::Output, Bitmap)>;
}

/// Defines basic multiplication operation for primitive arrays
pub trait ArrayMul<Rhs> {
    type Output;

    fn mul(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines checked multiplication operation for primitive arrays
pub trait ArrayCheckedMul<Rhs> {
    type Output;

    fn checked_mul(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines saturating multiplication operation for primitive arrays
pub trait ArraySaturatingMul<Rhs> {
    type Output;

    fn saturating_mul(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines Overflowing multiplication operation for primitive arrays
pub trait ArrayOverflowingMul<Rhs> {
    type Output;

    fn overflowing_mul(&self, rhs: &Rhs) -> Result<(Self::Output, Bitmap)>;
}

/// Defines basic division operation for primitive arrays
pub trait ArrayDiv<Rhs> {
    type Output;

    fn div(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines checked division operation for primitive arrays
pub trait ArrayCheckedDiv<Rhs> {
    type Output;

    fn checked_div(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines basic reminder operation for primitive arrays
pub trait ArrayRem<Rhs> {
    type Output;

    fn rem(&self, rhs: &Rhs) -> Result<Self::Output>;
}

/// Defines checked reminder operation for primitive arrays
pub trait ArrayCheckedRem<Rhs> {
    type Output;

    fn checked_rem(&self, rhs: &Rhs) -> Result<Self::Output>;
}

// The decimal primitive array defines different arithmetic functions and
// it requires specialization
pub unsafe trait NotI128 {}
unsafe impl NotI128 for u8 {}
unsafe impl NotI128 for u16 {}
unsafe impl NotI128 for u32 {}
unsafe impl NotI128 for u64 {}
unsafe impl NotI128 for i8 {}
unsafe impl NotI128 for i16 {}
unsafe impl NotI128 for i32 {}
unsafe impl NotI128 for i64 {}
unsafe impl NotI128 for f32 {}
unsafe impl NotI128 for f64 {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency() {
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
        let operators = vec![
            Operator::Add,
            Operator::Divide,
            Operator::Subtract,
            Operator::Multiply,
            Operator::Remainder,
        ];

        let cases = datatypes
            .clone()
            .into_iter()
            .zip(operators.into_iter())
            .zip(datatypes.into_iter());

        cases.for_each(|((lhs, op), rhs)| {
            let lhs_a = new_empty_array(lhs.clone());
            let rhs_a = new_empty_array(rhs.clone());
            if can_arithmetic(&lhs, op, &rhs) {
                assert!(arithmetic(lhs_a.as_ref(), op, rhs_a.as_ref()).is_ok());
            } else {
                assert!(arithmetic(lhs_a.as_ref(), op, rhs_a.as_ref()).is_err());
            }
        });
    }
}
