use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::{specification::check_offsets, Array, Offset};

#[derive(Debug)]
pub struct Utf8Array<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
}

impl<O: Offset> Utf8Array<O> {
    /// # Safety
    /// `values` buffer must contain valid utf8 between every `offset`
    pub unsafe fn from_data_unchecked(
        offsets: Buffer<O>,
        values: Buffer<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        Self {
            data_type: if O::is_large() {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            },
            offsets,
            values,
            validity,
        }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let len = (offset_1 - offset).to_usize().unwrap();
        // sound:
        let slice =
            std::slice::from_raw_parts(self.values.as_ptr().add(offset.to_usize().unwrap()), len);
        std::str::from_utf8_unchecked(slice)
    }
}

impl<O: Offset> Array for Utf8Array<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }
}

impl<'a, O: Offset> IntoIterator for &'a Utf8Array<O> {
    type Item = Option<&'a str>;
    type IntoIter = Utf8Iter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        Utf8Iter::new(self)
    }
}

impl<'a, O: Offset> Utf8Array<O> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> Utf8Iter<'a, O> {
        Utf8Iter::new(&self)
    }
}

/// an iterator that returns `Some(&str)` or `None`, for string arrays
#[derive(Debug)]
pub struct Utf8Iter<'a, T>
where
    T: Offset,
{
    array: &'a Utf8Array<T>,
    i: usize,
    len: usize,
}

impl<'a, T: Offset> Utf8Iter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a Utf8Array<T>) -> Self {
        Utf8Iter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: Offset> std::iter::Iterator for Utf8Iter<'a, T> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(unsafe { self.array.value_unchecked(i) }))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, T: Offset> std::iter::ExactSizeIterator for Utf8Iter<'a, T> {}
