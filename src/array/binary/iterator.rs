use crate::{array::Offset, bitmap::utils::ZipValidity, trusted_len::TrustedLen};

use super::BinaryArray;

/// Iterator over slices of `&[u8]`.
#[derive(Debug, Clone)]
pub struct BinaryValueIter<'a, O: Offset> {
    array: &'a BinaryArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> BinaryValueIter<'a, O> {
    /// Creates a new [`BinaryValueIter`]
    pub fn new(array: &'a BinaryArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for BinaryValueIter<'a, O> {
    type Item = &'a [u8];

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

impl<'a, O: Offset> DoubleEndedIterator for BinaryValueIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<'a, &'a [u8], BinaryValueIter<'a, O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<O: Offset> TrustedLen for BinaryValueIter<'_, O> {}
