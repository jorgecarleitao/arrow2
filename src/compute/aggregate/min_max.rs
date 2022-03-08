use crate::bitmap::utils::{BitChunkIterExact, BitChunksExact};
use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::scalar::*;
use crate::types::simd::*;
use crate::types::NativeType;
use crate::{
    array::{Array, BinaryArray, BooleanArray, Offset, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
};

/// Trait describing a type describing multiple lanes with an order relationship
/// consistent with the same order of `T`.
pub trait SimdOrd<T> {
    /// The minimum value
    const MIN: T;
    /// The maximum value
    const MAX: T;
    /// reduce itself to the minimum
    fn max_element(self) -> T;
    /// reduce itself to the maximum
    fn min_element(self) -> T;
    /// lane-wise maximum between two instances
    fn max_lane(self, x: Self) -> Self;
    /// lane-wise minimum between two instances
    fn min_lane(self, x: Self) -> Self;
    /// returns a new instance with all lanes equal to `MIN`
    fn new_min() -> Self;
    /// returns a new instance with all lanes equal to `MAX`
    fn new_max() -> Self;
}

/// Helper to compute min/max of [`BinaryArray`]
fn min_max_binary<O: Offset, F: Fn(&[u8], &[u8]) -> bool>(
    array: &BinaryArray<O>,
    cmp: F,
) -> Option<&[u8]> {
    let null_count = array.null_count();

    if null_count == array.len() || array.len() == 0 {
        return None;
    }
    let value = if array.validity().is_some() {
        array.iter().fold(None, |mut acc: Option<&[u8]>, v| {
            if let Some(item) = v {
                if let Some(acc) = acc.as_mut() {
                    if cmp(acc, item) {
                        *acc = item
                    }
                } else {
                    acc = Some(item)
                }
            }
            acc
        })
    } else {
        array
            .values_iter()
            .fold(None, |mut acc: Option<&[u8]>, item| {
                if let Some(acc) = acc.as_mut() {
                    if cmp(acc, item) {
                        *acc = item
                    }
                } else {
                    acc = Some(item)
                }
                acc
            })
    };
    value
}

/// Helper to compute min/max of [`Utf8Array`]
fn min_max_string<O: Offset, F: Fn(&str, &str) -> bool>(
    array: &Utf8Array<O>,
    cmp: F,
) -> Option<&str> {
    let null_count = array.null_count();

    if null_count == array.len() || array.len() == 0 {
        return None;
    }
    let value = if array.validity().is_some() {
        array.iter().fold(None, |mut acc: Option<&str>, v| {
            if let Some(item) = v {
                if let Some(acc) = acc.as_mut() {
                    if cmp(acc, item) {
                        *acc = item
                    }
                } else {
                    acc = Some(item)
                }
            }
            acc
        })
    } else {
        array
            .values_iter()
            .fold(None, |mut acc: Option<&str>, item| {
                if let Some(acc) = acc.as_mut() {
                    if cmp(acc, item) {
                        *acc = item
                    }
                } else {
                    acc = Some(item)
                }
                acc
            })
    };
    value
}

fn nonnull_min_primitive<T>(values: &[T]) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let chunks = values.chunks_exact(T::Simd::LANES);
    let remainder = chunks.remainder();

    let chunk_reduced = chunks.fold(T::Simd::new_min(), |acc, chunk| {
        let chunk = T::Simd::from_chunk(chunk);
        acc.min_lane(chunk)
    });

    let remainder = T::Simd::from_incomplete_chunk(remainder, T::Simd::MAX);
    let reduced = chunk_reduced.min_lane(remainder);

    reduced.min_element()
}

fn null_min_primitive_impl<T, I>(values: &[T], mut validity_masks: I) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
    I: BitChunkIterExact<<<T as Simd>::Simd as NativeSimd>::Chunk>,
{
    let mut chunks = values.chunks_exact(T::Simd::LANES);

    let chunk_reduced = chunks.by_ref().zip(validity_masks.by_ref()).fold(
        T::Simd::new_min(),
        |acc, (chunk, validity_chunk)| {
            let chunk = T::Simd::from_chunk(chunk);
            let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_chunk);
            let chunk = chunk.select(mask, T::Simd::new_min());
            acc.min_lane(chunk)
        },
    );

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::Simd::MAX);
    let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_masks.remainder());
    let remainder = remainder.select(mask, T::Simd::new_min());
    let reduced = chunk_reduced.min_lane(remainder);

    reduced.min_element()
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
fn null_min_primitive<T>(values: &[T], bitmap: &Bitmap) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let (slice, offset, length) = bitmap.as_slice();
    if offset == 0 {
        let validity_masks = BitChunksExact::<<T::Simd as NativeSimd>::Chunk>::new(slice, length);
        null_min_primitive_impl(values, validity_masks)
    } else {
        let validity_masks = bitmap.chunks::<<T::Simd as NativeSimd>::Chunk>();
        null_min_primitive_impl(values, validity_masks)
    }
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
fn null_max_primitive<T>(values: &[T], bitmap: &Bitmap) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let (slice, offset, length) = bitmap.as_slice();
    if offset == 0 {
        let validity_masks = BitChunksExact::<<T::Simd as NativeSimd>::Chunk>::new(slice, length);
        null_max_primitive_impl(values, validity_masks)
    } else {
        let validity_masks = bitmap.chunks::<<T::Simd as NativeSimd>::Chunk>();
        null_max_primitive_impl(values, validity_masks)
    }
}

fn nonnull_max_primitive<T>(values: &[T]) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let chunks = values.chunks_exact(T::Simd::LANES);
    let remainder = chunks.remainder();

    let chunk_reduced = chunks.fold(T::Simd::new_max(), |acc, chunk| {
        let chunk = T::Simd::from_chunk(chunk);
        acc.max_lane(chunk)
    });

    let remainder = T::Simd::from_incomplete_chunk(remainder, T::Simd::MIN);
    let reduced = chunk_reduced.max_lane(remainder);

    reduced.max_element()
}

fn null_max_primitive_impl<T, I>(values: &[T], mut validity_masks: I) -> T
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
    I: BitChunkIterExact<<<T as Simd>::Simd as NativeSimd>::Chunk>,
{
    let mut chunks = values.chunks_exact(T::Simd::LANES);

    let chunk_reduced = chunks.by_ref().zip(validity_masks.by_ref()).fold(
        T::Simd::new_max(),
        |acc, (chunk, validity_chunk)| {
            let chunk = T::Simd::from_chunk(chunk);
            let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_chunk);
            let chunk = chunk.select(mask, T::Simd::new_max());
            acc.max_lane(chunk)
        },
    );

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::Simd::MIN);
    let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_masks.remainder());
    let remainder = remainder.select(mask, T::Simd::new_max());
    let reduced = chunk_reduced.max_lane(remainder);

    reduced.max_element()
}

/// Returns the minimum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn min_primitive<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let null_count = array.null_count();

    // Includes case array.len() == 0
    if null_count == array.len() {
        return None;
    }
    let values = array.values();

    Some(if let Some(validity) = array.validity() {
        null_min_primitive(values, validity)
    } else {
        nonnull_min_primitive(values)
    })
}

/// Returns the maximum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn max_primitive<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Simd,
    T::Simd: SimdOrd<T>,
{
    let null_count = array.null_count();

    // Includes case array.len() == 0
    if null_count == array.len() {
        return None;
    }
    let values = array.values();

    Some(if let Some(validity) = array.validity() {
        null_max_primitive(values, validity)
    } else {
        nonnull_max_primitive(values)
    })
}

/// Returns the maximum value in the binary array, according to the natural order.
pub fn max_binary<O: Offset>(array: &BinaryArray<O>) -> Option<&[u8]> {
    min_max_binary(array, |a, b| a < b)
}

/// Returns the minimum value in the binary array, according to the natural order.
pub fn min_binary<O: Offset>(array: &BinaryArray<O>) -> Option<&[u8]> {
    min_max_binary(array, |a, b| a > b)
}

/// Returns the maximum value in the string array, according to the natural order.
pub fn max_string<O: Offset>(array: &Utf8Array<O>) -> Option<&str> {
    min_max_string(array, |a, b| a < b)
}

/// Returns the minimum value in the string array, according to the natural order.
pub fn min_string<O: Offset>(array: &Utf8Array<O>) -> Option<&str> {
    min_max_string(array, |a, b| a > b)
}

/// Returns the minimum value in the boolean array.
///
/// ```
/// use arrow2::{
///   array::BooleanArray,
///   compute::aggregate::min_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(min_boolean(&a), Some(false))
/// ```
pub fn min_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the min bool is false (0), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(false))
        .flatten()
        .or(Some(true))
}

/// Returns the maximum value in the boolean array
///
/// ```
/// use arrow2::{
///   array::BooleanArray,
///   compute::aggregate::max_boolean,
/// };
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(max_boolean(&a), Some(true))
/// ```
pub fn max_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the max bool is true (1), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(true))
        .flatten()
        .or(Some(false))
}

macro_rules! dyn_primitive {
    ($ty:ty, $array:expr, $f:ident) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Box::new(PrimitiveScalar::<$ty>::new(
            $array.data_type().clone(),
            $f::<$ty>(array),
        ))
    }};
}

macro_rules! dyn_generic {
    ($array_ty:ty, $scalar_ty:ty, $array:expr, $f:ident) => {{
        let array = $array.as_any().downcast_ref::<$array_ty>().unwrap();
        Box::new(<$scalar_ty>::new($f(array)))
    }};
}

/// Returns the maximum of [`Array`]. The scalar is null when all elements are null.
/// # Error
/// Errors iff the type does not support this operation.
pub fn max(array: &dyn Array) -> Result<Box<dyn Scalar>> {
    Ok(match array.data_type() {
        DataType::Boolean => dyn_generic!(BooleanArray, BooleanScalar, array, max_boolean),
        DataType::Int8 => dyn_primitive!(i8, array, max_primitive),
        DataType::Int16 => dyn_primitive!(i16, array, max_primitive),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(i32, array, max_primitive)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => dyn_primitive!(i64, array, max_primitive),
        DataType::UInt8 => dyn_primitive!(u8, array, max_primitive),
        DataType::UInt16 => dyn_primitive!(u16, array, max_primitive),
        DataType::UInt32 => dyn_primitive!(u32, array, max_primitive),
        DataType::UInt64 => dyn_primitive!(u64, array, max_primitive),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_primitive!(f32, array, max_primitive),
        DataType::Float64 => dyn_primitive!(f64, array, max_primitive),
        DataType::Decimal(_, _) => dyn_primitive!(i128, array, max_primitive),
        DataType::Utf8 => dyn_generic!(Utf8Array<i32>, Utf8Scalar<i32>, array, max_string),
        DataType::LargeUtf8 => dyn_generic!(Utf8Array<i64>, Utf8Scalar<i64>, array, max_string),
        DataType::Binary => dyn_generic!(BinaryArray<i32>, BinaryScalar<i32>, array, max_binary),
        DataType::LargeBinary => {
            dyn_generic!(BinaryArray<i64>, BinaryScalar<i64>, array, max_binary)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `max` operator does not support type `{:?}`",
                array.data_type(),
            )))
        }
    })
}

/// Returns the minimum of [`Array`]. The scalar is null when all elements are null.
/// # Error
/// Errors iff the type does not support this operation.
pub fn min(array: &dyn Array) -> Result<Box<dyn Scalar>> {
    Ok(match array.data_type() {
        DataType::Boolean => dyn_generic!(BooleanArray, BooleanScalar, array, min_boolean),
        DataType::Int8 => dyn_primitive!(i8, array, min_primitive),
        DataType::Int16 => dyn_primitive!(i16, array, min_primitive),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(i32, array, min_primitive)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => dyn_primitive!(i64, array, min_primitive),
        DataType::UInt8 => dyn_primitive!(u8, array, min_primitive),
        DataType::UInt16 => dyn_primitive!(u16, array, min_primitive),
        DataType::UInt32 => dyn_primitive!(u32, array, min_primitive),
        DataType::UInt64 => dyn_primitive!(u64, array, min_primitive),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_primitive!(f32, array, min_primitive),
        DataType::Float64 => dyn_primitive!(f64, array, min_primitive),
        DataType::Decimal(_, _) => dyn_primitive!(i128, array, min_primitive),
        DataType::Utf8 => dyn_generic!(Utf8Array<i32>, Utf8Scalar<i32>, array, min_string),
        DataType::LargeUtf8 => dyn_generic!(Utf8Array<i64>, Utf8Scalar<i64>, array, min_string),
        DataType::Binary => dyn_generic!(BinaryArray<i32>, BinaryScalar<i32>, array, min_binary),
        DataType::LargeBinary => {
            dyn_generic!(BinaryArray<i64>, BinaryScalar<i64>, array, min_binary)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `max` operator does not support type `{:?}`",
                array.data_type(),
            )))
        }
    })
}
