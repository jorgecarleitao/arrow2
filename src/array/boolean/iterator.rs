use crate::bitmap::utils::{zip_validity, BitmapIter, ZipValidity};

use super::super::MutableArray;
use super::{BooleanArray, MutableBooleanArray};

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<'a, bool, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a MutableBooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<'a, bool, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MutableBooleanArray {
    /// Returns an iterator over the optional values of this [`MutableBooleanArray`].
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, bool, BitmapIter<'a>> {
        zip_validity(
            self.values().iter(),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator over the values of this [`MutableBooleanArray`]
    #[inline]
    pub fn values_iter(&'a self) -> BitmapIter<'a> {
        self.values().iter()
    }
}
