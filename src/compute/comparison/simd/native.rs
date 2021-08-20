use std::convert::TryInto;

use super::{Simd8, Simd8Lanes};

#[inline]
fn set<T: Copy, F: Fn(T, T) -> bool>(lhs: [T; 8], rhs: [T; 8], op: F) -> u8 {
    let mut byte = 0u8;
    lhs.iter()
        .zip(rhs.iter())
        .enumerate()
        .for_each(|(i, (lhs, rhs))| {
            byte |= if op(*lhs, *rhs) { 1 << i } else { 0 };
        });
    byte
}

macro_rules! simd8 {
    ($type:ty) => {
        impl Simd8 for $type {
            type Simd = [$type; 8];
        }

        impl Simd8Lanes<$type> for [$type; 8] {
            #[inline]
            fn from_chunk(v: &[$type]) -> Self {
                v.try_into().unwrap()
            }

            #[inline]
            fn from_incomplete_chunk(v: &[$type], remaining: $type) -> Self {
                let mut a = [remaining; 8];
                a.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
                a
            }

            #[inline]
            fn eq(self, other: Self) -> u8 {
                set(self, other, |x, y| x == y)
            }

            #[inline]
            fn neq(self, other: Self) -> u8 {
                #[allow(clippy::float_cmp)]
                set(self, other, |x, y| x != y)
            }

            #[inline]
            fn lt_eq(self, other: Self) -> u8 {
                set(self, other, |x, y| x <= y)
            }

            #[inline]
            fn lt(self, other: Self) -> u8 {
                set(self, other, |x, y| x < y)
            }

            #[inline]
            fn gt_eq(self, other: Self) -> u8 {
                set(self, other, |x, y| x >= y)
            }

            #[inline]
            fn gt(self, other: Self) -> u8 {
                set(self, other, |x, y| x > y)
            }
        }
    };
}

simd8!(u8);
simd8!(u16);
simd8!(u32);
simd8!(u64);
simd8!(i8);
simd8!(i16);
simd8!(i32);
simd8!(i128);
simd8!(i64);
simd8!(f32);
simd8!(f64);
