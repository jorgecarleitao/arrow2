use crate::array::{ArrayAccessor, ArrayValuesIter, MutableArray};
use crate::bitmap::utils::{BitmapIter, ZipValidity};

use super::{FixedSizeBinaryArray, MutableFixedSizeBinaryArray};

unsafe impl<'a> ArrayAccessor<'a> for FixedSizeBinaryArray {
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

unsafe impl<'a> ArrayAccessor<'a> for MutableFixedSizeBinaryArray {
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

/// Iterator of values of an [`FixedSizeBinaryArray`].
pub type FixedSizeBinaryValuesIter<'a> = ArrayValuesIter<'a, FixedSizeBinaryArray>;
pub type MutableFixedSizeBinaryValuesIter<'a> = ArrayValuesIter<'a, MutableFixedSizeBinaryArray>;

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], FixedSizeBinaryValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> FixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> ZipValidity<&'a [u8], FixedSizeBinaryValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new(self.values_iter(), self.validity.as_ref().map(|x| x.iter()))
    }

    /// Returns iterator over the values of [`FixedSizeBinaryArray`]
    pub fn values_iter(&'a self) -> FixedSizeBinaryValuesIter<'a> {
        FixedSizeBinaryValuesIter::new(self)
    }
}

impl<'a> IntoIterator for &'a MutableFixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], MutableFixedSizeBinaryValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MutableFixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(
        &'a self,
    ) -> ZipValidity<&'a [u8], MutableFixedSizeBinaryValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new(
            self.iter_values(),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns iterator over the values of [`MutableFixedSizeBinaryArray`]
    pub fn iter_values(&'a self) -> MutableFixedSizeBinaryValuesIter<'a> {
        MutableFixedSizeBinaryValuesIter::new(self)
    }
}
