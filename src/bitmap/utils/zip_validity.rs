use crate::{bitmap::Bitmap, trusted_len::TrustedLen};

/// Iterator of Option<T> from an iterator and validity.
pub struct ZipValidity<'a, T, I: Iterator<Item = T>> {
    values: I,
    validity_iter: std::slice::Iter<'a, u8>,
    has_validity: bool,
    current_byte: &'a u8,
    validity_len: usize,
    validity_index: usize,
    mask: u8,
}

impl<'a, T, I: Iterator<Item = T>> ZipValidity<'a, T, I> {
    #[inline]
    pub fn new(values: I, validity: &'a Option<Bitmap>) -> Self {
        let offset = validity.as_ref().map(|x| x.offset()).unwrap_or(0);
        let bytes = validity
            .as_ref()
            .map(|x| &x.bytes()[offset / 8..])
            .unwrap_or(&[0]);

        let mut validity_iter = bytes.iter();

        let current_byte = validity_iter.next().unwrap_or(&0);

        Self {
            values,
            validity_iter,
            has_validity: validity.is_some(),
            mask: 1u8.rotate_left(offset as u32),
            validity_len: validity.as_ref().map(|x| x.len()).unwrap_or(0),
            validity_index: 0,
            current_byte,
        }
    }
}

impl<'a, T, I: Iterator<Item = T>> Iterator for ZipValidity<'a, T, I> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.has_validity {
            // easily predictable in branching
            if self.validity_index == self.validity_len {
                return None;
            } else {
                self.validity_index += 1;
            }
            let is_valid = self.current_byte & self.mask != 0;
            self.mask = self.mask.rotate_left(1);
            if self.mask == 1 {
                // reached a new byte => try to fetch it from the iterator
                match self.validity_iter.next() {
                    Some(v) => self.current_byte = v,
                    None => {
                        return if is_valid {
                            self.values.next().map(Some)
                        } else {
                            self.values.next();
                            Some(None)
                        }
                    }
                }
            }
            if is_valid {
                self.values.next().map(Some)
            } else {
                self.values.next();
                Some(None)
            }
        } else {
            self.values.next().map(Some)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
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
    use std::iter::FromIterator;

    use crate::bitmap::MutableBitmap;

    use super::*;

    #[test]
    fn basic() {
        let a: Option<Bitmap> = MutableBitmap::from_iter(vec![true, false]).into();
        let values = vec![0, 1];
        let zip = zip_validity(values.into_iter(), &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(a, vec![Some(0), None]);
    }

    #[test]
    fn complete() {
        let a: Option<Bitmap> =
            MutableBitmap::from_iter(vec![true, false, true, false, true, false, true, false])
                .into();
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
        let a: Option<Bitmap> = MutableBitmap::from_iter(vec![true, false]).into();
        let offsets = vec![0, 2, 3];
        let values = vec![1, 2, 3];
        let iter = (0..3).map(|x| {
            let start = offsets[x];
            let end = offsets[x + 1];
            &values[start..end]
        });
        let zip = zip_validity(iter, &a);

        let a = zip.collect::<Vec<_>>();
        assert_eq!(a, vec![Some([1, 2].as_ref()), None]);
    }

    #[test]
    fn byte() {
        let a: Option<Bitmap> = MutableBitmap::from_iter(vec![
            true, false, true, false, false, true, true, false, true,
        ])
        .into();
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
        let a: Bitmap = MutableBitmap::from_iter(vec![
            true, false, true, false, false, true, true, false, true,
        ])
        .into();
        let a = Some(a.slice(1, 8));
        let values = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
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
}
