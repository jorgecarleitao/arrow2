use crate::array::{binary::BinaryIter, IterableBinaryArray};

use super::FixedSizeBinaryArray;

impl IterableBinaryArray for FixedSizeBinaryArray {
    unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        Self::value_unchecked(self, i)
    }
}

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = BinaryIter<'a, FixedSizeBinaryArray>;

    fn into_iter(self) -> Self::IntoIter {
        BinaryIter::new(self)
    }
}

impl<'a> FixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BinaryIter<'a, Self> {
        BinaryIter::new(&self)
    }
}
