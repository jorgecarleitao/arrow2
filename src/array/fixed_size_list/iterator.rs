use crate::array::{list::iterator::ListIter, Array, IterableListArray};

use super::FixedSizeListArray;

impl IterableListArray for FixedSizeListArray {
    fn value(&self, i: usize) -> Box<dyn Array> {
        FixedSizeListArray::value(self, i)
    }
}

impl<'a> IntoIterator for &'a FixedSizeListArray {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ListIter<'a, FixedSizeListArray>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter::new(self)
    }
}

impl<'a> FixedSizeListArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> ListIter<'a, Self> {
        ListIter::new(&self)
    }
}
