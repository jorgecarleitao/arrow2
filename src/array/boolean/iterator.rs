use crate::array::Array;

use super::BooleanArray;

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = BooleanIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BooleanIter::<'a>::new(self)
    }
}

impl<'a> BooleanArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BooleanIter<'a> {
        BooleanIter::<'a>::new(&self)
    }
}

/// an iterator that returns Some(bool) or None.
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct BooleanIter<'a> {
    array: &'a BooleanArray,
    current: usize,
    current_end: usize,
}

impl<'a> BooleanIter<'a> {
    /// create a new iterator
    pub fn new(array: &'a BooleanArray) -> Self {
        BooleanIter {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> std::iter::Iterator for BooleanIter<'a> {
    type Item = Option<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.array.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            Some(Some(self.array.value(old)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a> std::iter::DoubleEndedIterator for BooleanIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                Some(self.array.value(self.current_end))
            })
        }
    }
}

/// all arrays have known size.
impl<'a> std::iter::ExactSizeIterator for BooleanIter<'a> {}
