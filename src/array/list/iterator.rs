use crate::array::{Array, IterableListArray};
use crate::bitmap::utils::{zip_validity, ZipValidity};
use crate::{array::Offset, trusted_len::TrustedLen};

use super::ListArray;

/// Iterator of values of an `ListArray`.
pub struct ListValuesIter<'a, A: IterableListArray> {
    array: &'a A,
    index: usize,
    end: usize,
}

impl<'a, A: IterableListArray> ListValuesIter<'a, A> {
    #[inline]
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, A: IterableListArray> Iterator for ListValuesIter<'a, A> {
    type Item = Box<dyn Array>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a, A: IterableListArray> TrustedLen for ListValuesIter<'a, A> {}

impl<'a, A: IterableListArray> DoubleEndedIterator for ListValuesIter<'a, A> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<O: Offset> IterableListArray for ListArray<O> {
    fn value(&self, i: usize) -> Box<dyn Array> {
        ListArray::<O>::value(self, i)
    }
}

type ValuesIter<'a, O> = ListValuesIter<'a, ListArray<O>>;
type ZipIter<'a, O> = ZipValidity<'a, Box<dyn Array>, ValuesIter<'a, O>>;

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
        zip_validity(ListValuesIter::new(self), &self.validity)
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> ValuesIter<'a, O> {
        ListValuesIter::new(self)
    }
}
