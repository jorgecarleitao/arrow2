use crate::array::Array;
use crate::bitmap::utils::{zip_validity, ZipValidity};
use crate::trusted_len::TrustedLen;

use super::{DictionaryArray, DictionaryKey};

/// Iterator of values of an `ListArray`.
pub struct DictionaryValuesIter<'a, K: DictionaryKey> {
    array: &'a DictionaryArray<K>,
    index: usize,
    end: usize,
}

impl<'a, K: DictionaryKey> DictionaryValuesIter<'a, K> {
    #[inline]
    pub fn new(array: &'a DictionaryArray<K>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, K: DictionaryKey> Iterator for DictionaryValuesIter<'a, K> {
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

unsafe impl<'a, K: DictionaryKey> TrustedLen for DictionaryValuesIter<'a, K> {}

impl<'a, K: DictionaryKey> DoubleEndedIterator for DictionaryValuesIter<'a, K> {
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

type ValuesIter<'a, K> = DictionaryValuesIter<'a, K>;
type ZipIter<'a, K> = ZipValidity<'a, Box<dyn Array>, ValuesIter<'a, K>>;

impl<'a, K: DictionaryKey> IntoIterator for &'a DictionaryArray<K> {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipIter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K: DictionaryKey> DictionaryArray<K> {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipIter<'a, K> {
        zip_validity(
            DictionaryValuesIter::new(self),
            self.keys.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> ValuesIter<'a, K> {
        DictionaryValuesIter::new(self)
    }
}
