use super::days_ms;
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

    #[inline]
    fn from_slice(v: &[Self]) -> Self::Simd {
        match v.try_into() {
            Ok(t) => t,
            _ => panic!(""),
        }
    }

    fn new_simd() -> Self::Simd;
}

unsafe impl Simd for u8 {
    const LANES: usize = 64;
    type Simd = [u8; 64];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 64]
    }
}

unsafe impl Simd for u16 {
    const LANES: usize = 32;
    type Simd = [u16; 32];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 32]
    }
}

unsafe impl Simd for u32 {
    const LANES: usize = 16;
    type Simd = [u32; 16];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 16]
    }
}

unsafe impl Simd for u64 {
    const LANES: usize = 8;
    type Simd = [u64; 8];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 8]
    }
}

unsafe impl Simd for i8 {
    const LANES: usize = 64;
    type Simd = [i8; 64];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 64]
    }
}

unsafe impl Simd for i16 {
    const LANES: usize = 32;
    type Simd = [i16; 32];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 32]
    }
}

unsafe impl Simd for i32 {
    const LANES: usize = 16;
    type Simd = [i32; 16];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 16]
    }
}

unsafe impl Simd for i64 {
    const LANES: usize = 8;
    type Simd = [i64; 8];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 8]
    }
}

unsafe impl Simd for f32 {
    const LANES: usize = 16;
    type Simd = [f32; 16];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0.0; 16]
    }
}

unsafe impl Simd for f64 {
    const LANES: usize = 8;
    type Simd = [f64; 8];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0.0; 8]
    }
}

unsafe impl Simd for i128 {
    const LANES: usize = 4;
    type Simd = [i128; 4];

    #[inline]
    fn new_simd() -> Self::Simd {
        [0; 4]
    }
}

unsafe impl Simd for days_ms {
    const LANES: usize = 8;
    type Simd = [days_ms; 8];

    #[inline]
    fn new_simd() -> Self::Simd {
        [days_ms([0i32; 2]); 8]
    }
}
