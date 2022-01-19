use crate::bitmap::utils::{zip_validity, ZipValidity};
use crate::{array::Offset, trusted_len::TrustedLen};

use super::StringSequenceArray;

/// Iterator of values of an `Utf8Array`.
#[derive(Debug, Clone)]
pub struct StringSequenceValuesIter<'a, O: Offset> {
    array: &'a StringSequenceArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> StringSequenceValuesIter<'a, O> {
    /// Creates a new [`StringSequenceValuesIter`]
    pub fn new(array: &'a StringSequenceArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for StringSequenceValuesIter<'a, O> {
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
}

impl<'a, O: Offset> DoubleEndedIterator for StringSequenceValuesIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a StringSequenceArray<O> {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<'a, &'a str, StringSequenceValuesIter<'a, O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> StringSequenceArray<O> {
    /// Returns an iterator of `Option<&str>`
    pub fn iter(&'a self) -> ZipValidity<'a, &'a str, StringSequenceValuesIter<'a, O>> {
        zip_validity(
            StringSequenceValuesIter::new(self),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `&str`
    pub fn values_iter(&'a self) -> StringSequenceValuesIter<'a, O> {
        StringSequenceValuesIter::new(self)
    }
}

unsafe impl<O: Offset> TrustedLen for StringSequenceValuesIter<'_, O> {}
