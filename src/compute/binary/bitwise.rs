use crate::array::{Array, BooleanArray, PrimitiveArray};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::types::NativeType;
use crate::compute::arithmetics::basic::check_same_type;
use crate::compute::arity::binary;

/// Performs `OR` operation on two arrays.
/// # Error
/// This function errors when the arrays have different lengths or are different types.
pub fn or<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
    where
        T: NativeType + BitwiseOr<Output = T>,
{
    check_same_type(lhs, rhs)?;
    binary(lhs, rhs, lhs.data_type().clone(), |a, b| a | b)
}

/// Performs `XOR` operation on two arrays.
/// # Error
/// This function errors when the arrays have different lengths or are different types.
pub fn xor<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
    where
        T: NativeType + BitAnd<Output = T>,
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
