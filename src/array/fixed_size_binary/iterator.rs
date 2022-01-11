use crate::bitmap::utils::{zip_validity, ZipValidity};

use super::super::MutableArray;
use super::{FixedSizeBinaryArray, FixedSizeBinaryValues, MutableFixedSizeBinaryArray};

/// # Safety
/// This iterator is `TrustedLen`
pub struct FixedSizeBinaryValuesIter<'a, T: FixedSizeBinaryValues> {
    array: &'a T,
    len: usize,
    index: usize,
}

impl<'a, T: FixedSizeBinaryValues> FixedSizeBinaryValuesIter<'a, T> {
    #[inline]
    pub fn new(array: &'a T) -> Self {
        Self {
            array,
            len: array.values().len() / array.size(),
            index: 0,
        }
    }
}

impl<'a, T: FixedSizeBinaryValues> Iterator for FixedSizeBinaryValuesIter<'a, T> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let index = self.index;
        let r = Some(unsafe {
            std::slice::from_raw_parts(
                self.array.values().as_ptr().add(index * self.array.size()),
                self.array.size(),
            )
        });
        self.index += 1;
        r
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.index, Some(self.len - self.index))
    }
}

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<'a, &'a [u8], FixedSizeBinaryValuesIter<'a, FixedSizeBinaryArray>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> FixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(
        &'a self,
    ) -> ZipValidity<'a, &'a [u8], FixedSizeBinaryValuesIter<'a, FixedSizeBinaryArray>> {
        zip_validity(
            FixedSizeBinaryValuesIter::new(self),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns iterator over the values of [`FixedSizeBinaryArray`]
    pub fn iter_values(&'a self) -> FixedSizeBinaryValuesIter<'a, FixedSizeBinaryArray> {
        FixedSizeBinaryValuesIter::new(self)
    }
}

impl<'a> IntoIterator for &'a MutableFixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter =
        ZipValidity<'a, &'a [u8], FixedSizeBinaryValuesIter<'a, MutableFixedSizeBinaryArray>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MutableFixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(
        &'a self,
    ) -> ZipValidity<'a, &'a [u8], FixedSizeBinaryValuesIter<'a, MutableFixedSizeBinaryArray>> {
        zip_validity(
            FixedSizeBinaryValuesIter::new(self),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns iterator over the values of [`MutableFixedSizeBinaryArray`]
    pub fn iter_values(&'a self) -> FixedSizeBinaryValuesIter<'a, MutableFixedSizeBinaryArray> {
        FixedSizeBinaryValuesIter::new(self)
    }
}
