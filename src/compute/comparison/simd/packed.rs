use std::convert::TryInto;

use super::{set, Simd8, Simd8Lanes};

use packed_simd::*;

macro_rules! simd8 {
    ($type:ty, $md:ty) => {
        impl Simd8 for $type {
            type Simd = $md;
        }

        impl Simd8Lanes<$type> for $md {
            #[inline]
            fn from_chunk(v: &[$type]) -> Self {
                <$md>::from_slice_unaligned(v)
            }

            #[inline]
            fn from_incomplete_chunk(v: &[$type], remaining: $type) -> Self {
                let mut a = [remaining; 8];
                a.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
                Self::from_chunk(a.as_ref())
            }

            #[inline]
            fn eq(self, other: Self) -> u8 {
                self.eq(other).bitmask()
            }

            #[inline]
            fn neq(self, other: Self) -> u8 {
                self.ne(other).bitmask()
            }

            #[inline]
            fn lt_eq(self, other: Self) -> u8 {
                self.le(other).bitmask()
            }

            #[inline]
            fn lt(self, other: Self) -> u8 {
                self.lt(other).bitmask()
            }

            #[inline]
            fn gt_eq(self, other: Self) -> u8 {
                self.ge(other).bitmask()
            }

            #[inline]
            fn gt(self, other: Self) -> u8 {
                self.gt(other).bitmask()
            }
        }
    };
}

simd8!(u8, u8x8);
simd8!(u16, u16x8);
simd8!(u32, u32x8);
simd8!(u64, u64x8);
simd8!(i8, i8x8);
simd8!(i16, i16x8);
simd8!(i32, i32x8);
simd8!(i64, i64x8);
simd8_native!(i128);
simd8!(f32, f32x8);
simd8!(f64, f64x8);
