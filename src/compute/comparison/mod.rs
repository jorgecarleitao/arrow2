//! Contains comparison operators
//!
//! The module contains functions that compare either an [`Array`] and a [`Scalar`]
//! or two [`Array`]s (of the same [`DataType`]). The scalar-oriented functions are
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
pub use simd::{Simd8, Simd8Lanes, Simd8PartialEq, Simd8PartialOrd};

pub(crate) use primitive::compare_values_op as primitive_compare_values_op;

macro_rules! match_eq_ord {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use crate::datatypes::PrimitiveType::*;
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        DaysMs => todo!(),
        MonthDayNano => todo!(),
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
    }
})}

macro_rules! match_eq {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use crate::datatypes::PrimitiveType::*;
    use crate::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        DaysMs => __with_ty__! { days_ms },
        MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
    }
})}

macro_rules! compare {
    ($lhs:expr, $rhs:expr, $op:tt, $p:tt) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        assert_eq!(
            lhs.data_type().to_logical_type(),
            rhs.data_type().to_logical_type()
        );

        use crate::datatypes::PhysicalType::*;
        match lhs.data_type().to_physical_type() {
            Boolean => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                boolean::$op(lhs, rhs)
            }
            Primitive(primitive) => $p!(primitive, |$T| {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                primitive::$op::<$T>(lhs, rhs)
            }),
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
            _ => todo!(
                "Comparison between {:?} are not yet supported",
                lhs.data_type()
            ),
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
    compare!(lhs, rhs, eq, match_eq)
}

/// `!=` between two [`Array`]s.
/// Use [`can_neq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn neq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, neq, match_eq)
}

/// `<` between two [`Array`]s.
/// Use [`can_lt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn lt(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, lt, match_eq_ord)
}

/// `<=` between two [`Array`]s.
/// Use [`can_lt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn lt_eq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, lt_eq, match_eq_ord)
}

/// `>` between two [`Array`]s.
/// Use [`can_gt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn gt(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, gt, match_eq_ord)
}

/// `>=` between two [`Array`]s.
/// Use [`can_gt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * the arrays do not have have the same logical type
/// * the arrays do not have the same length
/// * the operation is not supported for the logical type
pub fn gt_eq(lhs: &dyn Array, rhs: &dyn Array) -> BooleanArray {
    compare!(lhs, rhs, gt_eq, match_eq_ord)
}

macro_rules! compare_scalar {
    ($lhs:expr, $rhs:expr, $op:tt, $p:tt) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        assert_eq!(
            lhs.data_type().to_logical_type(),
            rhs.data_type().to_logical_type()
        );
        if !rhs.is_valid() {
            return BooleanArray::new_null(DataType::Boolean, lhs.len());
        }

        use crate::datatypes::PhysicalType::*;
        match lhs.data_type().to_physical_type() {
            Boolean => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BooleanScalar>().unwrap();
                // validity checked above
                boolean::$op(lhs, rhs.value().unwrap())
            }
            Primitive(primitive) => $p!(primitive, |$T| {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<PrimitiveScalar<$T>>().unwrap();
                primitive::$op::<$T>(lhs, rhs.value().unwrap())
            }),
            Utf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i32>>().unwrap();
                utf8::$op::<i32>(lhs, rhs.value().unwrap())
            }
            LargeUtf8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i64>>().unwrap();
                utf8::$op::<i64>(lhs, rhs.value().unwrap())
            }
            Binary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i32>>().unwrap();
                binary::$op::<i32>(lhs, rhs.value().unwrap())
            }
            LargeBinary => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i64>>().unwrap();
                binary::$op::<i64>(lhs, rhs.value().unwrap())
            }
            _ => todo!("Comparisons of {:?} are not yet supported", lhs.data_type()),
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
    compare_scalar!(lhs, rhs, eq_scalar, match_eq)
}

/// `!=` between an [`Array`] and a [`Scalar`].
/// Use [`can_neq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn neq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, neq_scalar, match_eq)
}

/// `<` between an [`Array`] and a [`Scalar`].
/// Use [`can_lt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn lt_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, lt_scalar, match_eq_ord)
}

/// `<=` between an [`Array`] and a [`Scalar`].
/// Use [`can_lt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn lt_eq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, lt_eq_scalar, match_eq_ord)
}

/// `>` between an [`Array`] and a [`Scalar`].
/// Use [`can_gt`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn gt_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, gt_scalar, match_eq_ord)
}

/// `>=` between an [`Array`] and a [`Scalar`].
/// Use [`can_gt_eq`] to check whether the operation is valid
/// # Panic
/// Panics iff either:
/// * they do not have have the same logical type
/// * the operation is not supported for the logical type
pub fn gt_eq_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> BooleanArray {
    compare_scalar!(lhs, rhs, gt_eq_scalar, match_eq_ord)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_eq(data_type: &DataType) -> bool {
    can_partial_eq(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_neq(data_type: &DataType) -> bool {
    can_partial_eq(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_lt(data_type: &DataType) -> bool {
    can_partial_eq_and_ord(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_lt_eq(data_type: &DataType) -> bool {
    can_partial_eq_and_ord(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_gt(data_type: &DataType) -> bool {
    can_partial_eq_and_ord(data_type)
}

/// Returns whether a [`DataType`] is comparable (either array or scalar).
pub fn can_gt_eq(data_type: &DataType) -> bool {
    can_partial_eq_and_ord(data_type)
}

// The list of operations currently supported.
fn can_partial_eq_and_ord(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth)
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

// The list of operations currently supported.
fn can_partial_eq(data_type: &DataType) -> bool {
    can_partial_eq_and_ord(data_type)
        || matches!(
            data_type.to_logical_type(),
            DataType::Interval(IntervalUnit::DayTime)
                | DataType::Interval(IntervalUnit::MonthDayNano)
        )
}
