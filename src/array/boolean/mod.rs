// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{bitmap::Bitmap, datatypes::DataType};

use super::{display_fmt, Array};

/// A [`BooleanArray`] is arrow's equivalent to `Vec<Option<bool>>`, i.e.
/// an array designed for highly performant operations on optionally nullable booleans,
/// The size of this struct is `O(1)` as all data is stored behind an `Arc`.
#[derive(Debug, Clone)]
pub struct BooleanArray {
    data_type: DataType,
    values: Bitmap,
    validity: Option<Bitmap>,
    offset: usize,
}

impl BooleanArray {
    /// Returns a new empty [`BooleanArray`].
    #[inline]
    pub fn new_empty() -> Self {
        Self::from_data(Bitmap::new(), None)
    }

    /// Returns a new [`BooleanArray`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(length: usize) -> Self {
        let bitmap = Bitmap::new_zeroed(length);
        Self::from_data(bitmap.clone(), Some(bitmap))
    }

    /// The canonical method to create a [`BooleanArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    #[inline]
    pub fn from_data(values: Bitmap, validity: Option<Bitmap>) -> Self {
        if let Some(ref validity) = validity {
            assert_eq!(values.len(), validity.len());
        }
        Self {
            data_type: DataType::Boolean,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns a slice of this [`BooleanArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to essentially increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
        }
    }

    /// Returns the element at index `i` as &str
    #[inline]
    pub fn value(&self, i: usize) -> bool {
        self.values.get_bit(i)
    }

    /// Returns the values bitmap of this [`BooleanArray`].
    #[inline]
    pub fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl Array for BooleanArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for BooleanArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), "BooleanArray", f, false)
    }
}

impl<P: AsRef<[Option<bool>]>> From<P> for BooleanArray {
    /// Creates a new [`BooleanArray`] out of a slice of Optional `bool`.
    #[inline]
    fn from(slice: P) -> Self {
        unsafe { Self::from_trusted_len_iter(slice.as_ref().iter().map(|x| x.as_ref())) }
    }
}

mod ffi;
mod iterator;
pub use iterator::*;

mod from;

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn basics() {
        let data = vec![Some(true), None, Some(false)];

        let array = BooleanArray::from_iter(data);

        assert_eq!(array.value(0), true);
        assert_eq!(array.value(1), false);
        assert_eq!(array.value(2), false);
        assert_eq!(array.values(), &Bitmap::from((&[0b00000001], 3)));
        assert_eq!(array.validity(), &Some(Bitmap::from((&[0b00000101], 3))));
        assert_eq!(array.is_valid(0), true);
        assert_eq!(array.is_valid(1), false);
        assert_eq!(array.is_valid(2), true);

        let array2 = BooleanArray::from_data(array.values().clone(), array.validity().clone());
        assert_eq!(array, array2);

        let array = array.slice(1, 2);
        assert_eq!(array.value(0), false);
        assert_eq!(array.value(1), false);
    }

    #[test]
    fn empty() {
        let array = BooleanArray::new_empty();
        assert_eq!(array.values().len(), 0);
        assert_eq!(array.validity(), &None);
    }
}
