use crate::{
    array::{list::ListValuesIter, Array, IterableListArray},
    bitmap::utils::{zip_validity, ZipValidity},
};

use super::FixedSizeListArray;

impl IterableListArray for FixedSizeListArray {
    fn value(&self, i: usize) -> Box<dyn Array> {
        FixedSizeListArray::value(self, i)
    }
}

type ValuesIter<'a> = ListValuesIter<'a, FixedSizeListArray>;
type ZipIter<'a> = ZipValidity<'a, Box<dyn Array>, ValuesIter<'a>>;

impl<'a> IntoIterator for &'a FixedSizeListArray {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> FixedSizeListArray {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipIter<'a> {
        zip_validity(ListValuesIter::new(self), &self.validity)
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> ValuesIter<'a> {
        ListValuesIter::new(self)
    }
}
