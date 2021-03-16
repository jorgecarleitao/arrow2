use crate::bitmap::BitmapIter;
use crate::bits::{zip_validity, ZipValidity};

use super::BooleanArray;

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<'a, bool, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> BooleanArray {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, bool, BitmapIter<'a>> {
        zip_validity(self.values().iter(), &self.validity)
    }

    /// Returns an iterator of `bool`
    #[inline]
    pub fn values_iter(&'a self) -> BitmapIter<'a> {
        BitmapIter::new(self.values())
    }
}
