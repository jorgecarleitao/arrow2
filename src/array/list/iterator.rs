use crate::array::Offset;
use crate::array::{Array, ArrayAccessor, ArrayValuesIter};
use crate::bitmap::utils::{zip_validity, ZipValidity};

use super::ListArray;

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for ListArray<O> {
    type Item = Box<dyn Array>;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of a [`ListArray`].
pub type ListValuesIter<'a, O> = ArrayValuesIter<'a, ListArray<O>>;

type ZipIter<'a, O> = ZipValidity<'a, Box<dyn Array>, ListValuesIter<'a, O>>;

impl<'a, O: Offset> IntoIterator for &'a ListArray<O> {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> ListArray<O> {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipIter<'a, O> {
        zip_validity(
            ListValuesIter::new(self),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> ListValuesIter<'a, O> {
        ListValuesIter::new(self)
    }
}
