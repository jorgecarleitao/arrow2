use std::convert::TryFrom;

use num::Num;

use crate::buffer::{Buffer, NativeType};

pub unsafe trait Offset: NativeType + Num + Ord + std::ops::AddAssign {
    fn is_large() -> bool;

    fn to_usize(&self) -> Option<usize>;
}

unsafe impl Offset for i32 {
    #[inline]
    fn is_large() -> bool {
        false
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        Some(*self as usize)
    }
}

unsafe impl Offset for i64 {
    #[inline]
    fn is_large() -> bool {
        true
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        usize::try_from(*self).ok()
    }
}

#[inline]
pub fn check_offsets<T: Offset>(offsets: &Buffer<T>, values_len: usize) -> usize {
    // Check that we can transmute the offset buffer
    assert!(
        offsets.len() >= 1,
        "The length of the offset buffer must be larger than 1"
    );
    let len = offsets.len() - 1;

    let offsets = offsets.as_slice();
    assert_eq!(offsets[0], T::default());

    let last_offset = offsets[len];
    let last_offset = last_offset
        .to_usize()
        .expect("The last offset of the array is larger than usize::MAX");

    assert_eq!(
        values_len, last_offset,
        "The length of the values must be equal to the last offset value"
    );
    len
}
