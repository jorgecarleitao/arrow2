use crate::bitmap::utils::ZipValidity;
use crate::{array::Offset, trusted_len::TrustedLen};

use super::Utf8Array;

/// Iterator of values of an `Utf8Array`.
#[derive(Debug, Clone)]
pub struct Utf8ValuesIter<'a, O: Offset> {
    array: &'a Utf8Array<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> Utf8ValuesIter<'a, O> {
    /// Creates a new [`Utf8ValuesIter`]
    pub fn new(array: &'a Utf8Array<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for Utf8ValuesIter<'a, O> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(unsafe { self.array.value_unchecked(old) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let new_index = self.index + n;
        if new_index > self.end {
            self.index = self.end;
            None
        } else {
            self.index = new_index;
            self.next()
        }
    }
}

impl<'a, O: Offset> DoubleEndedIterator for Utf8ValuesIter<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(unsafe { self.array.value_unchecked(self.end) })
        }
    }
}

impl<'a, O: Offset> IntoIterator for &'a Utf8Array<O> {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<'a, &'a str, Utf8ValuesIter<'a, O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<O: Offset> TrustedLen for Utf8ValuesIter<'_, O> {}
