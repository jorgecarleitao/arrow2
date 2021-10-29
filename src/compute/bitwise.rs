use std::ops::{BitAnd, BitOr, BitXor, Not};

use crate::array::{Array, PrimitiveArray};
use crate::compute::arithmetics::basic::check_same_type;
use crate::compute::arity::{binary, unary};
use crate::error::Result;
use crate::types::NativeType;

/// Performs `OR` operation on two arrays.
/// # Error
/// This function errors when the arrays have different lengths or are different types.
pub fn or<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + BitOr<Output = T>,
{
    check_same_type(lhs, rhs)?;
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a | b)
}

/// Performs `XOR` operation on two arrays.
/// # Error
/// This function errors when the arrays have different lengths or are different types.
pub fn xor<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + BitXor<Output = T>,
{
    check_same_type(lhs, rhs)?;
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a ^ b)
}

/// Performs `AND` operation on two arrays.
/// # Error
/// This function errors when the arrays have different lengths or are different types.
pub fn and<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: NativeType + BitAnd<Output = T>,
{
    check_same_type(lhs, rhs)?;
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a & b)
}

/// Performs `OR` operation on one array.
pub fn not<T>(arr: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + Not<Output = T>,
{
    let op = move |a: T| !a;
    unary(arr, op, arr.data_type().clone())
}
