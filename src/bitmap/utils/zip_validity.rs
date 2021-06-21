use crate::{bitmap::Bitmap, trusted_len::TrustedLen};

use super::BitmapIter;

/// An iterator adapter that converts an iterator over `T` and a validity
/// into an iterator over `Option<T>` based on the validity.
pub struct ZipValidity<'a, T, I: Iterator<Item = T>> {
    values: I,
    validity_iter: BitmapIter<'a>,
    has_validity: bool,
}

impl<'a, T, I: Iterator<Item = T>> ZipValidity<'a, T, I> {
    #[inline]
    pub fn new(values: I, validity: &'a Option<Bitmap>) -> Self {
        let validity_iter = validity
            .as_ref()
            .map(|x| x.iter())
            .unwrap_or_else(|| BitmapIter::new(&[], 0, 0));

        Self {
            values,
            validity_iter,
            has_validity: validity.is_some(),
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
#[inline]
pub fn zip_validity<T, I: Iterator<Item = T>>(
    values: I,
    validity: &Option<Bitmap>,
) -> ZipValidity<T, I> {
    ZipValidity::<T, I>::new(values, validity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let a = Some(Bitmap::from([true, false]));
        let values = vec![0, 1];
        let zip = zip_validity(values.into_iter(), &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(a, vec![Some(0), None]);
    }

    #[test]
    fn complete() {
        let a = Some(Bitmap::from([
            true, false, true, false, true, false, true, false,
        ]));
        let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let zip = zip_validity(values.into_iter(), &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(
            a,
            vec![Some(0), None, Some(2), None, Some(4), None, Some(6), None]
        );
    }

    #[test]
    fn slices() {
        let a = Some(Bitmap::from([true, false]));
        let offsets = vec![0, 2, 3];
        let values = vec![1, 2, 3];
        let iter = offsets.windows(2).map(|x| {
            let start = x[0];
            let end = x[1];
            &values[start..end]
        });
        let zip = zip_validity(iter, &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(a, vec![Some([1, 2].as_ref()), None]);
    }

    #[test]
    fn byte() {
        let a = Some(Bitmap::from([
            true, false, true, false, false, true, true, false, true,
        ]));
        let values = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        let zip = zip_validity(values.into_iter(), &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(
            a,
            vec![
                Some(0),
                None,
                Some(2),
                None,
                None,
                Some(5),
                Some(6),
                None,
                Some(8)
            ]
        );
    }

    #[test]
    fn offset() {
        let a = Bitmap::from([true, false, true, false, false, true, true, false, true]);
        let a = Some(a.slice(1, 8));
        let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let zip = zip_validity(values.into_iter(), &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(
            a,
            vec![None, Some(1), None, None, Some(4), Some(5), None, Some(7)]
        );
    }

    #[test]
    fn none() {
        let values = vec![0, 1, 2];
        let zip = zip_validity(values.into_iter(), &None);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(a, vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn rev() {
        let a = Bitmap::from([true, false, true, false, false, true, true, false, true]);
        let a = Some(a.slice(1, 8));
        let values = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let zip = zip_validity(values.into_iter(), &a);

        let result = zip.rev().collect::<Vec<_>>();
        let expected = vec![None, Some(1), None, None, Some(4), Some(5), None, Some(7)]
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        assert_eq!(result, expected);
    }
}
