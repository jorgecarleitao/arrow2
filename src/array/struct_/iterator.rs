use std::slice::Iter;
use std::sync::Arc;

use crate::{
    array::Array,
    bitmap::utils::{zip_validity, ZipValidity},
};

use super::StructArray;

impl<'a> IntoIterator for &'a StructArray {
    type Item = Option<&'a Arc<dyn Array>>;
    type IntoIter = ZipValidity<'a, &'a Arc<dyn Array>, Iter<'a, Arc<dyn Array>>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> StructArray {
    /// Returns an iterator over the optional values of [`StructArray`].
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, &'a Arc<dyn Array>, Iter<'a, Arc<dyn Array>>> {
        zip_validity(
            self.values().iter(),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of [`StructArray`].
    #[inline]
    pub fn values_iter(&'a self) -> Iter<'a, Arc<dyn Array>> {
        self.values().iter()
    }
}
