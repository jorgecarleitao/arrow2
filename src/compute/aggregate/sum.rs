use std::ops::Add;

use multiversion::multiversion;

use crate::bitmap::utils::{BitChunkIterExact, BitChunksExact};
use crate::types::simd::*;
use crate::types::NativeType;
use crate::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
};

/// Object that can reduce itself to a number. This is used in the context of SIMD to reduce
/// a MD (e.g. `[f32; 16]`) into a single number (`f32`).
pub trait Sum<T> {
    fn simd_sum(self) -> T;
}

#[multiversion]
#[clone(target = "x86_64+avx")]
fn nonnull_sum<T>(values: &[T]) -> T
where
    T: NativeType + Simd,
    T::Simd: Add<Output = T::Simd> + Sum<T>,
{
    let mut chunks = values.chunks_exact(T::Simd::LANES);

    let sum = chunks.by_ref().fold(T::Simd::default(), |acc, chunk| {
        acc + T::Simd::from_chunk(chunk)
    });

    let remainder = T::Simd::from_incomplete_chunk(chunks.remainder(), T::default());
    let reduced = sum + remainder;

    reduced.simd_sum()
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
    if bitmap.offset() == 0 {
        let validity_masks =
            BitChunksExact::<<T::Simd as NativeSimd>::Chunk>::new(bitmap.as_slice(), bitmap.len());
        null_sum_impl(values, validity_masks)
    } else {
        let validity_masks = bitmap.chunks::<<T::Simd as NativeSimd>::Chunk>();
        null_sum_impl(values, validity_masks)
    }
}

/// Returns the sum of values in the array.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T>
where
    T: NativeType + Simd,
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

#[cfg(test)]
mod tests {
    use super::super::super::arithmetics;
    use super::*;
    use crate::array::*;
    use crate::datatypes::DataType;

    #[test]
    fn test_primitive_array_sum() {
        let a = Primitive::from_slice(&[1, 2, 3, 4, 5]).to(DataType::Int32);
        assert_eq!(15, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Primitive::from_slice(&[1.1f64, 2.2, 3.3, 4.4, 5.5]).to(DataType::Float64);
        assert!((16.5 - sum(&a).unwrap()).abs() < f64::EPSILON);
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Int32Array::from(&[None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(10, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Int32Array::from(&[None, None, None]);
        assert_eq!(None, sum(&a));
    }

    #[test]
    fn test_primitive_array_sum_large_64() {
        let a: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(i) } else { None })
            .collect();
        let b: Int64Array = (1..=100)
            .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
            .collect();
        // create an array that actually has non-zero values at the invalid indices
        let c = arithmetics::basic::add::add(&a, &b).unwrap();
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }
}
