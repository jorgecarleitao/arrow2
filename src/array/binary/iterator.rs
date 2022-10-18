use crate::{
    array::{ArrayAccessor, ArrayValuesIter, Offset},
    bitmap::utils::{BitmapIter, ZipValidity},
};

use super::BinaryArray;

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for BinaryArray<O> {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type BinaryValueIter<'a, O> = ArrayValuesIter<'a, BinaryArray<O>>;

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], BinaryValueIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
