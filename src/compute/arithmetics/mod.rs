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
//! be performed on Primitive Arrays. These operations can be the building for
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

use std::ops::{Add, Div, Mul, Neg, Sub};

use num::Zero;

use crate::array::*;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::types::NativeType;

use super::arity::unary;

// Macro to evaluate match branch in arithmetic function.
// The macro is used to downcast both arrays to a primitive_array_type. If there
// is an error then an ArrowError is return with the data_type that cause it.
// It returns the result from the arithmetic_primitive function evaluated with
// the Operator selected
macro_rules! primitive_arithmetic_match {
    ($lhs: expr, $rhs: expr, $op: expr, $primitive_array_type: ty) => {{
        let res_lhs = $lhs
            .as_any()
            .downcast_ref::<$primitive_array_type>()
            .unwrap();

        let res_rhs = $rhs
            .as_any()
            .downcast_ref::<$primitive_array_type>()
            .unwrap();

        arithmetic_primitive(res_lhs, $op, res_rhs)
            .map(Box::new)
            .map(|x| x as Box<dyn Array>)
    }};
}

/// Function to execute an arithmetic operation with two arrays
/// It uses the enum Operator to select the type of operation that is going to
/// be performed with the two arrays
pub fn arithmetic(lhs: &dyn Array, op: Operator, rhs: &dyn Array) -> Result<Box<dyn Array>> {
    let data_type = lhs.data_type();
    if data_type != rhs.data_type() {
        return Err(ArrowError::NotYetImplemented(
            "Arithmetic is currently only supported for arrays of the same logical type"
                .to_string(),
        ));
    }
    match data_type {
        DataType::Int8 => primitive_arithmetic_match!(lhs, rhs, op, Int8Array),
        DataType::Int16 => primitive_arithmetic_match!(lhs, rhs, op, Int16Array),
        DataType::Int32 => primitive_arithmetic_match!(lhs, rhs, op, Int32Array),
        DataType::Int64 | DataType::Duration(_) => {
            primitive_arithmetic_match!(lhs, rhs, op, Int64Array)
        }
        DataType::UInt8 => primitive_arithmetic_match!(lhs, rhs, op, UInt8Array),
        DataType::UInt16 => primitive_arithmetic_match!(lhs, rhs, op, UInt16Array),
        DataType::UInt32 => primitive_arithmetic_match!(lhs, rhs, op, UInt32Array),
        DataType::UInt64 => primitive_arithmetic_match!(lhs, rhs, op, UInt64Array),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => primitive_arithmetic_match!(lhs, rhs, op, Float32Array),
        DataType::Float64 => primitive_arithmetic_match!(lhs, rhs, op, Float64Array),
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref::<Int128Array>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Int128Array>().unwrap();

            let res = match op {
                Operator::Add => decimal::add::add(lhs, rhs),
                Operator::Subtract => decimal::sub::sub(lhs, rhs),
                Operator::Multiply => decimal::mul::mul(lhs, rhs),
                Operator::Divide => decimal::div::div(lhs, rhs),
            };

            res.map(Box::new).map(|x| x as Box<dyn Array>)
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Arithmetics between {:?} is not supported",
            data_type
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

/// Perform arithmetic operations on two primitive arrays based on the Operator enum
#[inline]
fn arithmetic_primitive<T>(
    lhs: &PrimitiveArray<T>,
    op: Operator,
    rhs: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Div<Output = T> + Zero + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
{
    match op {
        Operator::Add => basic::add::add(lhs, rhs),
        Operator::Subtract => basic::sub::sub(lhs, rhs),
        Operator::Multiply => basic::mul::mul(lhs, rhs),
        Operator::Divide => basic::div::div(lhs, rhs),
    }
}

/// Performs primitive operation on an array and and scalar
#[inline]
pub fn arithmetic_primitive_scalar<T>(
    lhs: &PrimitiveArray<T>,
    op: Operator,
    rhs: &T,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + Div<Output = T> + Zero + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
{
    match op {
        Operator::Add => Ok(basic::add::add_scalar(lhs, rhs)),
        Operator::Subtract => Ok(basic::sub::sub_scalar(lhs, rhs)),
        Operator::Multiply => Ok(basic::mul::mul_scalar(lhs, rhs)),
        Operator::Divide => Ok(basic::div::div_scalar(lhs, rhs)),
    }
}

/// Negates values from array.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::negate;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![None, Some(6), None, Some(7)]).to(DataType::Int32);
/// let result = negate(&a);
/// let expected = Primitive::from(&vec![None, Some(-6), None, Some(-7)]).to(DataType::Int32);
/// assert_eq!(result, expected)
/// ```
#[inline]
pub fn negate<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + Neg<Output = T>,
{
    unary(array, |a| -a, array.data_type().clone())
}
