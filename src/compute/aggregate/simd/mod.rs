use super::SimdOrd;
use crate::types::simd::{i128x8, NativeSimd};

macro_rules! simd_ord_int {
    ($simd:tt, $type:ty) => {
        impl SimdOrd<$type> for $simd {
            const MIN: $type = <$type>::MIN;
            const MAX: $type = <$type>::MAX;

            #[inline]
            fn max_element(self) -> $type {
                self.0.iter().copied().fold(Self::MIN, <$type>::max)
            }

            #[inline]
            fn min_element(self) -> $type {
                self.0.iter().copied().fold(Self::MAX, <$type>::min)
            }

            #[inline]
            fn max_lane(self, x: Self) -> Self {
                let mut result = <$simd>::default();
                result
                    .0
                    .iter_mut()
                    .zip(self.0.iter())
                    .zip(x.0.iter())
                    .for_each(|((a, b), c)| *a = (*b).max(*c));
                result
            }

            #[inline]
            fn min_lane(self, x: Self) -> Self {
                let mut result = <$simd>::default();
                result
                    .0
                    .iter_mut()
                    .zip(self.0.iter())
                    .zip(x.0.iter())
                    .for_each(|((a, b), c)| *a = (*b).min(*c));
                result
            }

            #[inline]
            fn new_min() -> Self {
                Self([Self::MAX; <$simd>::LANES])
            }

            #[inline]
            fn new_max() -> Self {
                Self([Self::MIN; <$simd>::LANES])
            }
        }
    };
}

pub(super) use simd_ord_int;

simd_ord_int!(i128x8, i128);

#[cfg(not(feature = "simd"))]
mod native;
#[cfg(not(feature = "simd"))]
pub use native::*;
#[cfg(feature = "simd")]
mod packed;
#[cfg(feature = "simd")]
#[cfg_attr(docsrs, doc(cfg(feature = "simd")))]
pub use packed::*;
