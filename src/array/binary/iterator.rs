use crate::array::Array;
use crate::array::Offset;

use super::BinaryArray;

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = BinaryIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        BinaryIter::new(self)
    }
}

impl<'a, O: Offset> BinaryArray<O> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BinaryIter<'a, O> {
        BinaryIter::new(&self)
    }
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct BinaryIter<'a, O>
where
    O: Offset,
{
    array: &'a BinaryArray<O>,
    i: usize,
    len: usize,
}

impl<'a, O: Offset> BinaryIter<'a, O> {
    /// create a new iterator
    pub fn new(array: &'a BinaryArray<O>) -> Self {
        BinaryIter::<O> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, O: Offset> std::iter::Iterator for BinaryIter<'a, O> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(unsafe { self.array.value_unchecked(i) }))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, O: Offset> std::iter::ExactSizeIterator for BinaryIter<'a, O> {}
