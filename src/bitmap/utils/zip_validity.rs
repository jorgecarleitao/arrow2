use crate::trusted_len::TrustedLen;

use super::BitmapIter;

/// An iterator adapter that converts an iterator over `T` and a validity
/// into an iterator over `Option<T>` based on the validity.
pub struct ZipValidity<'a, T, I: Iterator<Item = T>> {
    values: I,
    validity_iter: BitmapIter<'a>,
    has_validity: bool,
}

impl<'a, T, I: Iterator<Item = T> + Clone> Clone for ZipValidity<'a, T, I> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            validity_iter: self.validity_iter.clone(),
            has_validity: self.has_validity,
        }
    }
}

impl<'a, T, I: Iterator<Item = T>> ZipValidity<'a, T, I> {
    /// Creates a new [`ZipValidity`].
    pub fn new(values: I, validity: Option<BitmapIter<'a>>) -> Self {
        let has_validity = validity.as_ref().is_some();
        let validity_iter = validity.unwrap_or_else(|| BitmapIter::new(&[], 0, 0));

        Self {
            values,
            validity_iter,
            has_validity,
        }
    }
}

impl<'a, T, I: Iterator<Item = T>> Iterator for ZipValidity<'a, T, I> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_validity {
            self.values.next().map(Some)
        } else {
            let is_valid = self.validity_iter.next();
            let value = self.values.next();
            is_valid.map(|x| if x { value } else { None })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if !self.has_validity {
            self.values.nth(n).map(Some)
        } else {
            let is_valid = self.validity_iter.nth(n);
            let value = self.values.nth(n);
            is_valid.map(|x| if x { value } else { None })
        }
    }
}

impl<'a, T, I: DoubleEndedIterator<Item = T>> DoubleEndedIterator for ZipValidity<'a, T, I> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.has_validity {
            self.values.next_back().map(Some)
        } else {
            let is_valid = self.validity_iter.next_back();
            let value = self.values.next_back();
            is_valid.map(|x| if x { value } else { None })
        }
    }
}

/// all arrays have known size.
impl<'a, T, I: Iterator<Item = T>> std::iter::ExactSizeIterator for ZipValidity<'a, T, I> {}

unsafe impl<T, I: TrustedLen<Item = T>> TrustedLen for ZipValidity<'_, T, I> {}

/// Returns an iterator adapter that returns Option<T> according to an optional validity.
pub fn zip_validity<T, I: Iterator<Item = T>>(
    values: I,
    validity: Option<BitmapIter>,
) -> ZipValidity<T, I> {
    ZipValidity::new(values, validity)
}
