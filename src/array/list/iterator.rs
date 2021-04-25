use crate::array::{Array, IterableListArray};
use crate::{array::Offset, trusted_len::TrustedLen};

use super::ListArray;

impl<O: Offset> IterableListArray for ListArray<O> {
    fn value(&self, i: usize) -> Box<dyn Array> {
        ListArray::<O>::value(self, i)
    }
}

impl<'a, O: Offset> IntoIterator for &'a ListArray<O> {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ListIter<'a, ListArray<O>>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter::new(self)
    }
}

impl<'a, O: Offset> ListArray<O> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> ListIter<'a, Self> {
        ListIter::new(&self)
    }
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct ListIter<'a, A>
where
    A: IterableListArray,
{
    array: &'a A,
    i: usize,
    len: usize,
}

impl<'a, A: IterableListArray> ListIter<'a, A> {
    /// create a new iterator
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, A: IterableListArray> std::iter::Iterator for ListIter<'a, A> {
    type Item = Option<Box<dyn Array>>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(self.array.value(i)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, A: IterableListArray> std::iter::ExactSizeIterator for ListIter<'a, A> {}

unsafe impl<A: IterableListArray> TrustedLen for ListIter<'_, A> {}
