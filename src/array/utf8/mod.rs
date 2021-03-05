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

use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::DataType,
};

use super::{
    display_fmt,
    specification::{check_offsets, check_offsets_and_utf8},
    Array, GenericBinaryArray, Offset,
};

/// A [`Utf8Array`] is arrow's equivalent of `Vec<Option<String>>`, i.e.
/// an array designed for highly performant operations on optionally nullable strings.
/// The size of this struct is `O(1)` as all data is stored behind an `Arc`.
/// # Example
/// ```
/// use std::iter::FromIterator;
/// use arrow2::array::Utf8Array;
/// fn main() {
///     let data = vec![Some("hello"), None, Some("hello2")];
///     let array = Utf8Array::<i32>::from_iter(data);
///     assert_eq!(array.value(0), "hello");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Utf8Array<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> Utf8Array<O> {
    /// Returns a new empty [`Utf8Array`].
    #[inline]
    pub fn new_empty() -> Self {
        unsafe { Self::from_data_unchecked(Buffer::from(&[O::zero()]), Buffer::new(), None) }
    }

    /// Returns a new [`Utf8Array`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(length: usize) -> Self {
        Self::from_data(
            Buffer::new_zeroed(length + 1),
            Buffer::new(),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// The canonical method to create a [`Utf8Array`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * The `offsets` and `values` are consistent
    /// * The `values` between `offsets` are utf8 encoded
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn from_data(offsets: Buffer<O>, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        check_offsets_and_utf8(&offsets, &values);
        if let Some(ref validity) = validity {
            assert_eq!(offsets.len(), validity.len() + 1);
        }

        Self {
            data_type: if O::is_large() {
                DataType::LargeUtf8
            } else {
                DataType::Utf8
            },
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    /// The same as [`Utf8Array::from_data`] but does not check for utf8.
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
            offset: 0,
        }
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        let slice = std::slice::from_raw_parts(self.values.as_ptr().add(offset), length);
        // todo: validate utf8 so that we can use the unsafe version
        std::str::from_utf8(slice).unwrap()
    }

    /// Returns a slice of this [`Utf8Array`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to essentially increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        // + 1: `length == 0` implies that we take the first offset.
        let offsets = self.offsets.clone().slice(offset, length + 1);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            values: self.values.clone(),
            validity,
            offset: self.offset + offset,
        }
    }

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> &str {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        let slice = &self.values.as_slice()[offset..offset + length];
        // todo: validate utf8 so that we can use the unsafe version
        std::str::from_utf8(slice).unwrap()
    }

    /// Returns the offsets of this [`Utf8Array`].
    #[inline]
    pub fn offsets(&self) -> &[O] {
        self.offsets.as_slice()
    }

    /// Returns the offset [`Buffer`] of this [`Utf8Array`].
    #[inline]
    pub fn offsets_buffer(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// Returns the values of this [`Utf8Array`] as a slice of `u8`
    #[inline]
    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    /// Returns the values [`Buffer`] of this [`Utf8Array`]
    #[inline]
    pub fn values_buffer(&self) -> &Buffer<u8> {
        &self.values
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

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl<O: Offset> std::fmt::Display for Utf8Array<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display_fmt(self.iter(), &format!("{}", self.data_type()), f, false)
    }
}

impl<O: Offset> GenericBinaryArray<O> for Utf8Array<O> {
    #[inline]
    fn values(&self) -> &[u8] {
        self.values()
    }

    #[inline]
    fn offsets(&self) -> &[O] {
        self.offsets()
    }
}

mod ffi;
mod from;
pub use from::*;
mod iterator;
pub use iterator::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn basics() {
        let data = vec![Some("hello"), None, Some("hello2")];

        let array = Utf8Array::<i32>::from_iter(data);

        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(1), "");
        assert_eq!(array.value(2), "hello2");
        assert_eq!(unsafe { array.value_unchecked(2) }, "hello2");
        assert_eq!(array.values(), b"hellohello2");
        assert_eq!(array.offsets(), &[0, 5, 5, 11]);
        assert_eq!(array.validity(), &Some(Bitmap::from((&[0b00000101], 3))));
        assert_eq!(array.is_valid(0), true);
        assert_eq!(array.is_valid(1), false);
        assert_eq!(array.is_valid(2), true);

        let array2 = Utf8Array::<i32>::from_data(
            array.offsets_buffer().clone(),
            array.values_buffer().clone(),
            array.validity().clone(),
        );
        assert_eq!(array, array2);

        let array = array.slice(1, 2);
        assert_eq!(array.value(0), "");
        assert_eq!(array.value(1), "hello2");
        // note how this keeps everything: the offsets were sliced
        assert_eq!(array.values(), b"hellohello2");
        assert_eq!(array.offsets(), &[5, 5, 11]);
    }

    #[test]
    fn empty() {
        let array = Utf8Array::<i32>::new_empty();
        assert_eq!(array.values(), b"");
        assert_eq!(array.offsets(), &[0]);
        assert_eq!(array.validity(), &None);
    }
}
