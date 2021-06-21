use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, MutableArray, Offset, TryExtend},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::Utf8Array;

/// The mutable version of [`Utf8Array`]. See [`MutableArray`] for more details.
#[derive(Debug)]
pub struct MutableUtf8Array<O: Offset> {
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl<O: Offset> From<MutableUtf8Array<O>> for Utf8Array<O> {
    fn from(other: MutableUtf8Array<O>) -> Self {
        Utf8Array::<O>::from_data(
            other.offsets.into(),
            other.values.into(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl<O: Offset> Default for MutableUtf8Array<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// Initializes a new empty [`MutableUtf8Array`].
    pub fn new() -> Self {
        let mut offsets = MutableBuffer::<O>::new();
        offsets.push(O::default());
        Self {
            offsets,
            values: MutableBuffer::<u8>::new(),
            validity: None,
        }
    }

    /// Initializes a new [`MutableUtf8Array`] with a pre-allocated capacity of slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableUtf8Array`] with a pre-allocated capacity of slots and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());

        Self {
            offsets,
            values: MutableBuffer::<u8>::with_capacity(values),
            validity: None,
        }
    }

    /// Reserves `additional` elements and `additional_values` on the values buffer.
    pub fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.offsets.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
        self.values.reserve(additional_values);
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    /// Tries to push a new element to the array.
    /// # Error
    /// This operation errors iff the length of all values (in bytes) exceeds `O` maximum value.
    pub fn try_push<T: AsRef<str>>(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                let bytes = value.as_ref().as_bytes();
                self.values.extend_from_slice(bytes);

                let size = O::from_usize(self.values.len()).ok_or(ArrowError::KeyOverflowError)?;

                self.offsets.push(size);

                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {
                        self.set_validity();
                    }
                }
            }
            None => {
                self.offsets.push(self.last_offset());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// Pushes a new element to the array.
    /// # Panic
    /// This operation panics iff the length of all values (in bytes) exceeds `O` maximum value.
    pub fn push<T: AsRef<str>>(&mut self, value: Option<T>) {
        self.try_push(value).unwrap()
    }

    fn set_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(false)
                .take(self.len() - 1)
                .chain(std::iter::once(true)),
        ))
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// returns its values.
    pub fn values(&self) -> &MutableBuffer<u8> {
        &self.values
    }

    /// returns its offsets.
    pub fn offsets(&self) -> &MutableBuffer<O> {
        &self.offsets
    }
}

impl<O: Offset> MutableArray for MutableUtf8Array<O> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(Utf8Array::from_data(
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
    }

    fn data_type(&self) -> &DataType {
        if O::is_large() {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn push_null(&mut self) {
        self.push::<&str>(None)
    }
}

impl<O: Offset, P: AsRef<str>> FromIterator<Option<P>> for MutableUtf8Array<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter).unwrap()
    }
}

impl<O: Offset> MutableUtf8Array<O> {
    /// Creates a new [`MutableUtf8Array`] from an iterator.
    /// # Error
    /// This operation errors iff the total length in bytes on the iterator exceeds `O`'s maximum value.
    /// (`i32::MAX` or `i64::MAX` respectively).
    fn try_from_iter<P: AsRef<str>, I: IntoIterator<Item = Option<P>>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut array = Self::with_capacity(lower);
        for item in iterator {
            array.try_push(item)?;
        }
        Ok(array)
    }
}

impl<O: Offset, T: AsRef<str>> Extend<Option<T>> for MutableUtf8Array<O> {
    fn extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) {
        self.try_extend(iter).unwrap();
    }
}

impl<O: Offset, T: AsRef<str>> TryExtend<Option<T>> for MutableUtf8Array<O> {
    fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> Result<()> {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0, 0);
        iter.try_for_each(|x| self.try_push(x))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacities() {
        let b = MutableUtf8Array::<i32>::with_capacities(1, 10);

        assert_eq!(b.values.capacity(), 64);
        assert_eq!(b.offsets.capacity(), 16); // 64 bytes
    }
}
