//! Contains bitwise operators: [`or`], [`and`], [`xor`] and [`not`].
use std::ops::{BitAnd, BitOr, BitXor, Not};

use crate::array::PrimitiveArray;
use crate::compute::arity::{binary, unary};
use crate::types::NativeType;

/// Performs `OR` operation on two arrays.
/// # Panic
/// This function errors when the arrays have different lengths.
pub fn or<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + BitOr<Output = T>,
{
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a | b)
}

/// Performs `XOR` operation between two arrays.
/// # Panic
/// This function errors when the arrays have different lengths.
pub fn xor<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + BitXor<Output = T>,
{
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a ^ b)
}

/// Performs `AND` operation on two arrays.
/// # Panic
/// This function panics when the arrays have different lengths.
pub fn and<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + BitAnd<Output = T>,
{
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a & b)
}

/// Returns a new [`PrimitiveArray`] with the bitwise `not`.
pub fn not<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: NativeType + Not<Output = T>,
{
    let op = move |a: T| !a;
    unary(array, op, array.data_type().clone())
}
