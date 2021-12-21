use std::ops::Add;

use multiversion::multiversion;

use crate::bitmap::utils::{BitChunkIterExact, BitChunksExact};
use crate::datatypes::{DataType, IntervalUnit};
use crate::error::{ArrowError, Result};
use crate::scalar::*;
use crate::types::simd::*;
use crate::types::NativeType;
use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
};

/// Object that can reduce itself to a number. This is used in the context of SIMD to reduce
/// a MD (e.g. `[f32; 16]`) into a single number (`f32`).
pub trait Sum<T> {
    /// Reduces this element to a single value.
    fn simd_sum(self) -> T;
}

#[multiversion]
#[clone(target = "x86_64+avx")]
fn nonnull_sum<T>(values: &[T]) -> T
where
    T: NativeType + Simd + Add<Output = T> + std::iter::Sum<T>,
    T::Simd: Sum<T> + Add<Output = T::Simd>,
{
    let (head, simd_vals, tail) = T::Simd::align(values);

    let mut reduced = T::Simd::from_incomplete_chunk(&[], T::default());
    for chunk in simd_vals {
        reduced = reduced + *chunk;
    }

    reduced.simd_sum() + head.iter().copied().sum() + tail.iter().copied().sum()
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
#[multiversion]
#[clone(target = "x86_64+avx")]
fn null_sum_impl<T, I>(values: &[T], mut validity_masks: I) -> T
where
    T: NativeType + Simd,
    T::Simd: Add<Output = T::Simd> + Sum<T>,
    I: BitChunkIterExact<<<T as Simd>::Simd as NativeSimd>::Chunk>,
{
    let mut chunks = values.chunks_exact(T::Simd::LANES);

    let sum = chunks.by_ref().zip(validity_masks.by_ref()).fold(
        T::Simd::default(),
        |acc, (chunk, validity_chunk)| {
            let chunk = T::Simd::from_chunk(chunk);
            let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_chunk);
            let selected = chunk.select(mask, T::Simd::default());
            acc + selected
        },
    );

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::default());
    let mask = <T::Simd as NativeSimd>::Mask::from_chunk(validity_masks.remainder());
    let remainder = remainder.select(mask, T::Simd::default());
    let reduced = sum + remainder;

    reduced.simd_sum()
}

/// # Panics
/// iff `values.len() != bitmap.len()` or the operation overflows.
fn null_sum<T>(values: &[T], bitmap: &Bitmap) -> T
where
    T: NativeType + Simd,
    T::Simd: Add<Output = T::Simd> + Sum<T>,
{
    let (slice, offset, length) = bitmap.as_slice();
    if offset == 0 {
        let validity_masks = BitChunksExact::<<T::Simd as NativeSimd>::Chunk>::new(slice, length);
        null_sum_impl(values, validity_masks)
    } else {
        let validity_masks = bitmap.chunks::<<T::Simd as NativeSimd>::Chunk>();
        null_sum_impl(values, validity_masks)
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn sum_primitive<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Simd + Add<Output = T> + std::iter::Sum<T>,
    T::Simd: Add<Output = T::Simd> + Sum<T>,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return None;
    }

    match array.validity() {
        None => Some(nonnull_sum(array.values())),
        Some(bitmap) => Some(null_sum(array.values(), bitmap)),
    }
}

macro_rules! dyn_sum {
    ($ty:ty, $array:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Box::new(PrimitiveScalar::<$ty>::new(
            $array.data_type().clone(),
            sum_primitive::<$ty>(array),
        ))
    }};
}

/// Whether [`sum`] is valid for `data_type`
pub fn can_sum(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Int8 | Int16
            | Date32
            | Time32(_)
            | Interval(IntervalUnit::YearMonth)
            | Int64
            | Date64
            | Time64(_)
            | Timestamp(_, _)
            | Duration(_)
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
    )
}

/// Returns the sum of all elements in `array` as a [`Scalar`] of the same physical
/// and logical types as `array`.
/// # Error
/// Errors iff the operation is not supported.
pub fn sum(array: &dyn Array) -> Result<Box<dyn Scalar>> {
    Ok(match array.data_type() {
        DataType::Int8 => dyn_sum!(i8, array),
        DataType::Int16 => dyn_sum!(i16, array),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_sum!(i32, array)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => dyn_sum!(i64, array),
        DataType::UInt8 => dyn_sum!(u8, array),
        DataType::UInt16 => dyn_sum!(u16, array),
        DataType::UInt32 => dyn_sum!(u32, array),
        DataType::UInt64 => dyn_sum!(u64, array),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_sum!(f32, array),
        DataType::Float64 => dyn_sum!(f64, array),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The `sum` operator does not support type `{:?}`",
                array.data_type(),
            )))
        }
    })
}
