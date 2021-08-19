use crate::{
    bitmap::utils::{zip_validity, ZipValidity},
    types::NativeType,
};

use super::super::MutableArray;
use super::{MutablePrimitiveArray, PrimitiveArray};

impl<'a, T: NativeType> IntoIterator for &'a PrimitiveArray<T> {
    type Item = Option<&'a T>;
    type IntoIter = ZipValidity<'a, &'a T, std::slice::Iter<'a, T>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: NativeType> PrimitiveArray<T> {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, &'a T, std::slice::Iter<'a, T>> {
        zip_validity(
            self.values().iter(),
            self.validity.as_ref().map(|x| x.iter()),
        )
    }
}

impl<'a, T: NativeType> MutablePrimitiveArray<T> {
    /// Returns an iterator over `Option<T>`
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, &'a T, std::slice::Iter<'a, T>> {
        zip_validity(
            self.values().iter(),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `bool`
    #[inline]
    pub fn values_iter(&'a self) -> std::slice::Iter<'a, T> {
        self.values().iter()
    }
}
