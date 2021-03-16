use std::{
    fmt::Binary,
    ops::{BitAnd, BitAndAssign, BitOr, Not, Shl, ShlAssign, ShrAssign},
};

/// Something that can be use as a chunk of bits.
/// Currently implemented for `u8` and `u64`
/// # Safety
/// Do not implement.
pub unsafe trait BitChunk:
    Sized
    + Copy
    + std::fmt::Debug
    + Binary
    + BitAnd<Output = Self>
    + ShlAssign
    + Not<Output = Self>
    + ShrAssign<u32>
    + ShlAssign<u32>
    + Shl<u32, Output = Self>
    + Eq
    + BitAndAssign
    + BitOr<Output = Self>
{
    fn one() -> Self;
    fn zero() -> Self;
    fn to_le(self) -> Self;
}

unsafe impl BitChunk for u8 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u64 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}
