use crate::types::BitChunkIter;
use std::convert::TryInto;

use super::*;

macro_rules! simd {
    ($name:tt, $type:ty, $lanes:expr, $mask:ty) => {
        #[allow(non_camel_case_types)]
        pub struct $name(pub [$type; $lanes]);

        impl NativeSimd for $name {
            const LANES: usize = $lanes;
            type Native = $type;
            type Chunk = $mask;
            type Mask = $mask;

            #[inline]
            fn select(self, mask: $mask, default: Self) -> Self {
                let mut reduced = default;
                let iter = BitChunkIter::new(mask, Self::LANES);
                for (i, b) in (0..Self::LANES).zip(iter) {
                    reduced[i] = if b { self[i] } else { reduced[i] };
                }
                reduced
            }

            #[inline]
            fn from_chunk(v: &[$type]) -> Self {
                ($name)(v.try_into().unwrap())
            }

            #[inline]
            fn from_incomplete_chunk(v: &[$type], remaining: $type) -> Self {
                let mut a = [remaining; $lanes];
                a.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
                Self(a)
            }
        }

        impl std::ops::Index<usize> for $name {
            type Output = $type;

            #[inline]
            fn index(&self, index: usize) -> &Self::Output {
                &self.0[index]
            }
        }

        impl std::ops::IndexMut<usize> for $name {
            #[inline]
            fn index_mut(&mut self, index: usize) -> &mut Self::Output {
                &mut self.0[index]
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> Self {
                ($name)([<$type>::default(); $lanes])
            }
        }
    };
}

simd!(u8x64, u8, 64, u64);
simd!(u16x32, u16, 32, u32);
simd!(u32x16, u32, 16, u16);
simd!(u64x8, u64, 8, u8);
simd!(i8x64, i8, 64, u64);
simd!(i16x32, i16, 32, u32);
simd!(i32x16, i32, 16, u16);
simd!(i64x8, i64, 8, u8);
simd!(f32x16, f32, 16, u16);
simd!(f64x8, f64, 8, u8);

// In the native implementation, a mask is 1 bit wide, as per AVX512.
impl<T: BitChunk> FromMaskChunk<T> for T {
    #[inline]
    fn from_chunk(v: T) -> Self {
        v
    }
}
