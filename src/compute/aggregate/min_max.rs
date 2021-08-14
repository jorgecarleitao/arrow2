use crate::bitmap::utils::{BitChunkIterExact, BitChunksExact};
use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::scalar::*;
use crate::types::simd::*;
use crate::types::NativeType;
use crate::{
    array::{Array, BooleanArray, Offset, PrimitiveArray, Utf8Array},
    bitmap::Bitmap,
};

pub trait SimdOrd<T> {
    const MIN: T;
    const MAX: T;
    fn max_element(self) -> T;
    fn min_element(self) -> T;
    fn max(self, x: Self) -> Self;
    fn min(self, x: Self) -> Self;
    fn new_min() -> Self;
    fn new_max() -> Self;
}

/// Helper macro to perform min/max of strings
fn min_max_string<O: Offset, F: Fn(&str, &str) -> bool>(
    array: &Utf8Array<O>,
    cmp: F,
) -> Option<&str> {
    let null_count = array.null_count();

    if null_count == array.len() || array.len() == 0 {
        return None;
    }
    let mut n;
    if let Some(validity) = array.validity() {
        n = "";
        let mut has_value = false;

        for i in 0..array.len() {
            let item = array.value(i);
            if validity.get_bit(i) && (!has_value || cmp(n, item)) {
                has_value = true;
                n = item;
            }
        }
    } else {
        // array.len() == 0 checked above
        n = unsafe { array.value_unchecked(0) };
        for i in 1..array.len() {
            // loop is up to `len`.
            let item = unsafe { array.value_unchecked(i) };
            if cmp(n, item) {
                n = item;
            }
        }
    }
    Some(n)
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
        acc.min(chunk)
    });

    let remainder = T::Simd::from_incomplete_chunk(remainder, T::Simd::MAX);
    let reduced = chunk_reduced.min(remainder);

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
            acc.min(chunk)
        },
    );

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::Simd::MAX);
    let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_masks.remainder());
    let remainder = remainder.select(mask, T::Simd::new_min());
    let reduced = chunk_reduced.min(remainder);

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
        acc.max(chunk)
    });

    let remainder = T::Simd::from_incomplete_chunk(remainder, T::Simd::MIN);
    let reduced = chunk_reduced.max(remainder);

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
            acc.max(chunk)
        },
    );

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::Simd::MIN);
    let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_masks.remainder());
    let remainder = remainder.select(mask, T::Simd::new_max());
    let reduced = chunk_reduced.max(remainder);

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
        DataType::Utf8 => dyn_generic!(Utf8Array<i32>, Utf8Scalar<i32>, array, max_string),
        DataType::LargeUtf8 => dyn_generic!(Utf8Array<i64>, Utf8Scalar<i64>, array, max_string),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `max` operator does not support type `{}`",
                array.data_type(),
            )))
        }
    })
}

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
        DataType::Utf8 => dyn_generic!(Utf8Array<i32>, Utf8Scalar<i32>, array, min_string),
        DataType::LargeUtf8 => dyn_generic!(Utf8Array<i64>, Utf8Scalar<i64>, array, min_string),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `max` operator does not support type `{}`",
                array.data_type(),
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;

    #[test]
    fn test_primitive_array_min_max() {
        let a = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        assert_eq!(5, min_primitive(&a).unwrap());
        assert_eq!(9, max_primitive(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_min_max_with_nulls() {
        let a = Int32Array::from(&[Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, min_primitive(&a).unwrap());
        assert_eq!(9, max_primitive(&a).unwrap());
    }

    #[test]
    fn test_primitive_min_max_1() {
        let a = Int32Array::from(&[None, None, Some(5), Some(2)]);
        assert_eq!(Some(2), min_primitive(&a));
        assert_eq!(Some(5), max_primitive(&a));
    }

    #[test]
    fn min_max_f32() {
        let a = Float32Array::from(&[None, None, Some(5.0), Some(2.0)]);
        assert_eq!(Some(2.0), min_primitive(&a));
        assert_eq!(Some(5.0), max_primitive(&a));
    }

    #[test]
    fn min_max_f64() {
        let a = Float64Array::from(&[None, None, Some(5.0), Some(2.0)]);
        assert_eq!(Some(2.0), min_primitive(&a));
        assert_eq!(Some(5.0), max_primitive(&a));
    }

    #[test]
    fn min_max_f64_large() {
        // in simd, f64 has 8 lanes, thus > 8 covers the branch with lanes
        let a = Float64Array::from(&[
            None,
            None,
            Some(8.0),
            Some(2.0),
            None,
            None,
            Some(5.0),
            Some(2.0),
        ]);
        assert_eq!(Some(2.0), min_primitive(&a));
        assert_eq!(Some(8.0), max_primitive(&a));
    }

    #[test]
    fn min_max_f64_nan_only() {
        let a = Float64Array::from(&[None, Some(f64::NAN)]);
        assert!(min_primitive(&a).unwrap().is_nan());
        assert!(max_primitive(&a).unwrap().is_nan());
    }

    #[test]
    fn min_max_f64_nan() {
        let a = Float64Array::from(&[None, Some(1.0), Some(f64::NAN)]);
        assert_eq!(Some(1.0), min_primitive(&a));
        assert_eq!(Some(1.0), max_primitive(&a));
    }

    #[test]
    fn min_max_f64_edge_cases() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NEG_INFINITY)).collect();
        assert_eq!(Some(f64::NEG_INFINITY), min_primitive(&a));
        assert_eq!(Some(f64::NEG_INFINITY), max_primitive(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MIN)).collect();
        assert_eq!(Some(f64::MIN), min_primitive(&a));
        assert_eq!(Some(f64::MIN), max_primitive(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MAX)).collect();
        assert_eq!(Some(f64::MAX), min_primitive(&a));
        assert_eq!(Some(f64::MAX), max_primitive(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::INFINITY)).collect();
        assert_eq!(Some(f64::INFINITY), min_primitive(&a));
        assert_eq!(Some(f64::INFINITY), max_primitive(&a));
    }

    // todo: convert me
    #[test]
    fn test_string_min_max_with_nulls() {
        let a = Utf8Array::<i32>::from(&[Some("b"), None, None, Some("a"), Some("c")]);
        assert_eq!("a", min_string(&a).unwrap());
        assert_eq!("c", max_string(&a).unwrap());
    }

    #[test]
    fn test_string_min_max_all_nulls() {
        let a = Utf8Array::<i32>::from(&[None::<&str>, None]);
        assert_eq!(None, min_string(&a));
        assert_eq!(None, max_string(&a));
    }

    #[test]
    fn test_string_min_max_1() {
        let a = Utf8Array::<i32>::from(&[None, None, Some("b"), Some("a")]);
        assert_eq!(Some("a"), min_string(&a));
        assert_eq!(Some("b"), max_string(&a));
    }

    #[test]
    fn test_boolean_min_max_empty() {
        let a = BooleanArray::new_empty();
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_all_null() {
        let a = BooleanArray::from(&[None, None]);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_no_null() {
        let a = BooleanArray::from(&[Some(true), Some(false), Some(true)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max() {
        let a = BooleanArray::from(&[Some(true), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(&[None, Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(&[Some(false), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_smaller() {
        let a = BooleanArray::from(&[Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(&[None, Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(&[None, Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(&[Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }
}
