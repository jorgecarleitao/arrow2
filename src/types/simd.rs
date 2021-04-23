use super::days_ms;
use super::BitChunk;
use std::convert::TryInto;

/// A trait describing types that can be represented by SIMD instructions.
/// `Simd::Simd` is guaranteed to be `[T: NativeType; Simd::LANES]`.
/// # Safety
/// Do not implement.
pub unsafe trait Simd: Sized {
    const LANES: usize;
    type Simd: std::ops::Index<usize, Output = Self>
        + std::ops::IndexMut<usize, Output = Self>
        + for<'a> std::convert::TryFrom<&'a [Self]>;
    type SimdMask: BitChunk;

    #[inline]
    fn from_slice(v: &[Self]) -> Self::Simd {
        match v.try_into() {
            Ok(t) => t,
            _ => panic!(""),
        }
    }

    fn new_simd() -> Self::Simd;
}

macro_rules! native {
    ($type:ty, $lanes:tt, $mask:ty, $zero:tt) => {
        unsafe impl Simd for $type {
            const LANES: usize = $lanes;
            type Simd = [$type; $lanes];
            type SimdMask = $mask;

            #[inline]
            fn new_simd() -> Self::Simd {
                [$zero; $lanes]
            }
        }
    };
}

native!(u8, 64, u64, 0);
native!(u16, 32, u32, 0);
native!(u32, 16, u16, 0);
native!(u64, 8, u8, 0);
native!(i8, 64, u64, 0);
native!(i16, 32, u32, 0);
native!(i32, 16, u16, 0);
native!(i64, 8, u8, 0);
native!(f32, 16, u16, 0.0);
native!(f64, 8, u8, 0.0);
// this is not 512 as it needs to be at least 8bits to align well with masks.
native!(i128, 8, u8, 0);

static A: days_ms = days_ms([0i32; 2]);
native!(days_ms, 8, u8, A);
