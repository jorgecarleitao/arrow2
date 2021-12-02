//! Basic comparison kernels.
//!
//! The module contains functions that compare either an array and a scalar
//! or two arrays of the same [`DataType`]. The scalar-oriented functions are
//! suffixed with `_scalar`.
//!
//! The functions are organized in two variants:
//! * statically typed
//! * dynamically typed
//! The statically typed are available under each module of this module (e.g. [`primitive::eq`], [`primitive::lt_scalar`])
//! The dynamically typed are available in this module (e.g. [`eq`] or [`lt_scalar`]).
//!
//! # Examples
//!
//! Compare two [`PrimitiveArray`]s:
//! ```
//! use arrow2::array::{BooleanArray, PrimitiveArray};
//! use arrow2::compute::comparison::primitive::gt;
//!
//! let array1 = PrimitiveArray::<i32>::from([Some(1), None, Some(2)]);
//! let array2 = PrimitiveArray::<i32>::from([Some(1), Some(3), Some(1)]);
//! let result = gt(&array1, &array2);
//! assert_eq!(result, BooleanArray::from([Some(false), None, Some(true)]));
//! ```
//!
//! Compare two dynamically-typed [`Array`]s (trait objects):
//! ```
//! use arrow2::array::{Array, BooleanArray, PrimitiveArray};
//! use arrow2::compute::comparison::eq;
//!
//! let array1: &dyn Array = &PrimitiveArray::<f64>::from(&[Some(10.0), None, Some(20.0)]);
//! let array2: &dyn Array = &PrimitiveArray::<f64>::from(&[Some(10.0), None, Some(10.0)]);
//! let result = eq(array1, array2);
//! assert_eq!(result, BooleanArray::from([Some(true), None, Some(false)]));
//! ```
//!
//! Compare (not equal) a [`Utf8Array`] to a word:
//! ```
//! use arrow2::array::{BooleanArray, Utf8Array};
//! use arrow2::compute::comparison::utf8::neq_scalar;
//!
//! let array = Utf8Array::<i32>::from([Some("compute"), None, Some("compare")]);
//! let result = neq_scalar(&array, "compare");
//! assert_eq!(result, BooleanArray::from([Some(true), None, Some(false)]));
//! ```

use crate::array::*;
use crate::datatypes::{DataType, IntervalUnit};
use crate::scalar::*;

pub mod binary;
pub mod boolean;
pub mod primitive;
pub mod utf8;

mod simd;
pub use simd::{Simd8, Simd8Lanes};

pub(crate) use primitive::compare_values_op as primitive_compare_values_op;

macro_rules! compare {
    ($lhs:expr, $rhs:expr, $op:tt) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        assert_eq!(
            lhs.data_type().to_logical_type(),
            rhs.data_type().to_logical_type()
        );

        use DataType::*;
        let data_type = lhs.data_type().to_logical_type();
        match data_type {
            Boolean => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                boolean::$op(lhs, rhs)
            }
            Int8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<i8>(lhs, rhs)
            }
            Int16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<i16>(lhs, rhs)
            }
            Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<i32>(lhs, rhs)
            }
            Int64 | Timestamp(_, _) | Date64 | Time64(_) | Duration(_) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<i64>(lhs, rhs)
            }
            UInt8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<u8>(lhs, rhs)
            }
            UInt16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<u16>(lhs, rhs)
            }
            UInt32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<u32>(lhs, rhs)
            }
            UInt64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<u64>(lhs, rhs)
            }
            Float16 => unreachable!(),
            Float32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<f32>(lhs, rhs)
            }
            Float64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<f64>(lhs, rhs)
            }
            Utf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                utf8::$op::<i32>(lhs, rhs)
            }
            LargeUtf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                utf8::$op::<i64>(lhs, rhs)
            }
            Decimal(_, _) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<i128>(lhs, rhs)
            }
            Binary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                binary::$op::<i32>(lhs, rhs)
            }
            LargeBinary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                binary::$op::<i64>(lhs, rhs)
            }
            _ => todo!("Comparisons of {:?} are not yet supported", data_type),
        }
    }};
}

/// `==` between two [`Array`]s.
/// Use [`can_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn eq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, eq)
}

/// `!=` between two [`Array`]s.
/// Use [`can_neq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn neq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, neq)
}

/// `<` between two [`Array`]s.
/// Use [`can_lt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn lt(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, lt)
}

/// `<=` between two [`Array`]s.
/// Use [`can_lt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn lt_eq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, lt_eq)
}

/// `>` between two [`Array`]s.
/// Use [`can_gt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn gt(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, gt)
}

/// `>=` between two [`Array`]s.
/// Use [`can_gt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn gt_eq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, gt_eq)
}

macro_rules! compare_scalar {
    ($lhs:expr, $rhs:expr, $op:tt) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        assert_eq!(
            lhs.data_type().to_logical_type(),
            rhs.data_type().to_logical_type()
        );
        if !rhs.is_valid() {
            return BooleanArray::new_null(DataType::Boolean, lhs.len());
        }

        use DataType::*;
        let data_type = lhs.data_type().to_logical_type();
        match data_type {
            Boolean => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BooleanScalar>().unwrap();
                // validity checked above
                boolean::$op(lhs, rhs.value().unwrap())
            }
            Int8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<i8>>().unwrap();
                primitive::$op::<i8>(lhs, rhs.value().unwrap())
            }
            Int16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<i16>>().unwrap();
                primitive::$op::<i16>(lhs, rhs.value().unwrap())
            }
            Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<i32>>().unwrap();
                primitive::$op::<i32>(lhs, rhs.value().unwrap())
            }
            Int64 | Timestamp(_, _) | Date64 | Time64(_) | Duration(_) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<i64>>().unwrap();
                primitive::$op::<i64>(lhs, rhs.value().unwrap())
            }
            UInt8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<u8>>().unwrap();
                primitive::$op::<u8>(lhs, rhs.value().unwrap())
            }
            UInt16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<u16>>().unwrap();
                primitive::$op::<u16>(lhs, rhs.value().unwrap())
            }
            UInt32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<u32>>().unwrap();
                primitive::$op::<u32>(lhs, rhs.value().unwrap())
            }
            UInt64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<u64>>().unwrap();
                primitive::$op::<u64>(lhs, rhs.value().unwrap())
            }
            Float16 => unreachable!(),
            Float32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<f32>>().unwrap();
                primitive::$op::<f32>(lhs, rhs.value().unwrap())
            }
            Float64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<f64>>().unwrap();
                primitive::$op::<f64>(lhs, rhs.value().unwrap())
            }
            Utf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i32>>().unwrap();
                utf8::$op::<i32>(lhs, rhs.value())
            }
            LargeUtf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i64>>().unwrap();
                utf8::$op::<i64>(lhs, rhs.value())
            }
            Decimal(_, _) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<i128>>()
                    .unwrap();
                primitive::$op::<i128>(lhs, rhs.value().unwrap())
            }
            Binary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i32>>().unwrap();
                binary::$op::<i32>(lhs, rhs.value())
            }
            LargeBinary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i64>>().unwrap();
                binary::$op::<i64>(lhs, rhs.value())
            }
            _ => todo!("Comparisons of {:?} are not yet supported", data_type),
        }
    }};
}

/// `==` between an [`Array`] and a [`Scalar`].
/// Use [`can_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn eq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, eq_scalar)
}

/// `!=` between an [`Array`] and a [`Scalar`].
/// Use [`can_neq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn neq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, neq_scalar)
}

/// `<` between an [`Array`] and a [`Scalar`].
/// Use [`can_lt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn lt_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, lt_scalar)
}

/// `<=` between an [`Array`] and a [`Scalar`].
/// Use [`can_lt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn lt_eq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, lt_eq_scalar)
}

/// `>` between an [`Array`] and a [`Scalar`].
/// Use [`can_gt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn gt_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, gt_scalar)
}

/// `>=` between an [`Array`] and a [`Scalar`].
/// Use [`can_gt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn gt_eq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, gt_eq_scalar)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_eq(data_type: &DataType) -> bool {
    can_compare(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_neq(data_type: &DataType) -> bool {
    can_compare(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_lt(data_type: &DataType) -> bool {
    can_compare(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_lt_eq(data_type: &DataType) -> bool {
    can_compare(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_gt(data_type: &DataType) -> bool {
    can_compare(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar) comparison.
pub fn can_gt_eq(data_type: &DataType) -> bool {
    can_compare(data_type)
}

// The list of operations currently supported.
fn can_compare(data_type: &DataType) -> bool {
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
            | DataType::Timestamp(_, _)
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
