use std::convert::TryInto;

use crate::types::simd::*;

use crate::types::{days_ms, months_days_ns};

use super::*;

macro_rules! simd8 {
    ($type:ty, $md:ty) => {
        impl Simd8 for $type {
            type Simd = $md;
        }

        impl Simd8Lanes<$type> for $md {
            #[inline]
            fn from_chunk(v: &[$type]) -> Self {
                <$md>::from_slice(v)
            }

            #[inline]
            fn from_incomplete_chunk(v: &[$type], remaining: $type) -> Self {
                let mut a = [remaining; 8];
                a.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
                Self::from_array(a)
            }
        }

        impl Simd8PartialEq for $md {
            #[inline]
            fn eq(self, other: Self) -> u8 {
                to_bitmask(self.lanes_eq(other))
            }

            #[inline]
            fn neq(self, other: Self) -> u8 {
                to_bitmask(self.lanes_ne(other))
            }
        }

        impl Simd8PartialOrd for $md {
            #[inline]
            fn lt_eq(self, other: Self) -> u8 {
                to_bitmask(self.lanes_le(other))
            }

            #[inline]
            fn lt(self, other: Self) -> u8 {
                to_bitmask(self.lanes_lt(other))
            }

            #[inline]
            fn gt_eq(self, other: Self) -> u8 {
                to_bitmask(self.lanes_ge(other))
            }

            #[inline]
            fn gt(self, other: Self) -> u8 {
                to_bitmask(self.lanes_gt(other))
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
simd8_native_all!(i128);
simd8!(f32, f32x8);
simd8!(f64, f64x8);
simd8_native!(days_ms);
simd8_native_partial_eq!(days_ms);
simd8_native!(months_days_ns);
simd8_native_partial_eq!(months_days_ns);

fn to_bitmask<T: std::simd::MaskElement>(mask: std::simd::Mask<T, 8>) -> u8 {
    mask.test(0) as u8
        | ((mask.test(1) as u8) << 1)
        | ((mask.test(2) as u8) << 2)
        | ((mask.test(3) as u8) << 3)
        | ((mask.test(4) as u8) << 4)
        | ((mask.test(5) as u8) << 5)
        | ((mask.test(6) as u8) << 6)
        | ((mask.test(7) as u8) << 7)
}
