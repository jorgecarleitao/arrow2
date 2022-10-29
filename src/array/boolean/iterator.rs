use crate::bitmap::utils::{BitmapIter, ZipValidity};
use crate::bitmap::IntoIter;

use super::super::MutableArray;
use super::{BooleanArray, MutableBooleanArray};

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<bool, IntoIter, IntoIter>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        let (_, values, validity) = self.into_inner();
        let values = values.into_iter();
        let null_count = validity.as_ref().map(|validity| validity.unset_bits());
        let validity = validity.map(|x| x.into_iter());
        ZipValidity::new(values, validity, null_count)
    }
}

impl<'a> IntoIterator for &'a MutableBooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MutableBooleanArray {
    /// Returns an iterator over the optional values of this [`MutableBooleanArray`].
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>> {
        let null_count = self
            .validity()
            .as_ref()
            .map(|validity| validity.unset_bits());
        ZipValidity::new(
            self.values().iter(),
            self.validity().as_ref().map(|x| x.iter()),
            null_count,
        )
    }

    /// Returns an iterator over the values of this [`MutableBooleanArray`]
    #[inline]
    pub fn values_iter(&'a self) -> BitmapIter<'a> {
        self.values().iter()
    }
}
