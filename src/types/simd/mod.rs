//! Contains traits and implementations of multi-data used in SIMD.
//! The actual representation is driven by the feature flag `"simd"`, which, if set,
//! uses `packed_simd2` to get the intrinsics.
use super::{BitChunk, NativeType};

/// Describes the ability to convert itself from a [`BitChunk`].
pub trait FromMaskChunk<T> {
    /// Convert itself from a slice.
    fn from_chunk(v: T) -> Self;
}

/// A struct that lends itself well to be compiled leveraging SIMD
pub trait NativeSimd: Default {
    /// Number of lanes
    const LANES: usize;
    /// The [`NativeType`] of this struct. E.g. `f32` for a `NativeSimd = f32x16`.
    type Native: NativeType;
    /// The type holding bits for masks.
    type Chunk: BitChunk;
    /// Type used for masking.
    type Mask: FromMaskChunk<Self::Chunk>;

    /// Sets values to `default` based on `mask`.
    fn select(self, mask: Self::Mask, default: Self) -> Self;

    /// Convert itself from a slice.
    /// # Panics
    /// * iff `v.len()` != `T::LANES`
    fn from_chunk(v: &[Self::Native]) -> Self;

    /// Convert itself from a slice.
    /// # Safety:
    /// Caller must ensure:
    /// * `v.len() == T::LANES`
    /// * slice is aligned to `Self`
    unsafe fn from_chunk_aligned_unchecked(v: &[Self::Native]) -> Self;

    /// creates a new Self from `v` by populating items from `v` up to its length.
    /// Items from `v` at positions larger than the number of lanes are ignored;
    /// remaining items are populated with `remaining`.
    fn from_incomplete_chunk(v: &[Self::Native], remaining: Self::Native) -> Self;
}

/// Trait implemented by some [`NativeType`] that have a SIMD representation.
pub trait Simd: NativeType {
    /// The SIMD type associated with this trait.
    /// This type supports SIMD operations
    type Simd: NativeSimd<Native = Self>;
}

#[cfg(not(feature = "simd"))]
mod native;
#[cfg(not(feature = "simd"))]
pub use native::*;
#[cfg(feature = "simd")]
mod packed;
#[cfg(feature = "simd")]
pub use packed::*;

macro_rules! native {
    ($type:ty, $simd:ty) => {
        impl Simd for $type {
            type Simd = $simd;
        }
    };
}

native!(u8, u8x64);
native!(u16, u16x32);
native!(u32, u32x16);
native!(u64, u64x8);
native!(i8, i8x64);
native!(i16, i16x32);
native!(i32, i32x16);
native!(i64, i64x8);
native!(f32, f32x16);
native!(f64, f64x8);
