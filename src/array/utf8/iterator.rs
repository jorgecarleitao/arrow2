use crate::bitmap::utils::{zip_validity, ZipValidity};
use crate::{
    array::{Array, Offset},
    trusted_len::TrustedLen,
};

use super::Utf8Array;

/// Iterator of values of an `Utf8Array`.
pub struct Utf8ValuesIter<'a, O: Offset> {
    array: &'a Utf8Array<O>,
    index: usize,
}

impl<'a, O: Offset> Utf8ValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a Utf8Array<O>) -> Self {
        Self { array, index: 0 }
    }
}

impl<'a, O: Offset> Iterator for Utf8ValuesIter<'a, O> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            return None;
        } else {
            self.index += 1;
        }
        Some(unsafe { self.array.value_unchecked(self.index - 1) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.index,
            Some(self.array.len() - self.index),
        )
    }
}

impl<'a, O: Offset> IntoIterator for &'a Utf8Array<O> {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<'a, &'a str, Utf8ValuesIter<'a, O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> Utf8Array<O> {
    /// Returns an iterator of `Option<&str>`
    pub fn iter(&'a self) -> ZipValidity<'a, &'a str, Utf8ValuesIter<'a, O>> {
        zip_validity(Utf8ValuesIter::new(self), &self.validity)
    }

    /// Returns an iterator of `&str`
    pub fn values_iter(&'a self) -> Utf8ValuesIter<'a, O> {
        Utf8ValuesIter::new(self)
    }
}

unsafe impl<O: Offset> TrustedLen for Utf8ValuesIter<'_, O> {}
