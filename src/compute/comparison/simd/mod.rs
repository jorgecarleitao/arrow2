mod native;

pub use native::*;

use crate::types::NativeType;

/// [`NativeType`] that supports a representation of 8 lanes
pub trait Simd8: NativeType {
    type Simd: Simd8Lanes<Self>;
}

pub trait Simd8Lanes<T> {
    fn from_chunk(v: &[T]) -> Self;
    fn from_incomplete_chunk(v: &[T], remaining: T) -> Self;
    fn eq(self, other: Self) -> u8;
    fn neq(self, other: Self) -> u8;
    fn lt_eq(self, other: Self) -> u8;
    fn lt(self, other: Self) -> u8;
    fn gt(self, other: Self) -> u8;
    fn gt_eq(self, other: Self) -> u8;
}
