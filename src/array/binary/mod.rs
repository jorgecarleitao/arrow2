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
    ffi::ArrowArray,
};

use super::{
    display_fmt, display_helper, ffi::ToFFI, specification::check_offsets, specification::Offset,
    Array, FromFFI,
};

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct BinaryArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<O: Offset> BinaryArray<O> {
    pub fn new_empty() -> Self {
        Self::from_data(Buffer::from(&[O::zero()]), Buffer::new(), None)
    }

    pub fn from_data(offsets: Buffer<O>, values: Buffer<u8>, validity: Option<Bitmap>) -> Self {
        check_offsets(&offsets, values.len());

        Self {
            data_type: if O::is_large() {
                DataType::LargeBinary
            } else {
                DataType::Binary
            },
            offsets,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
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
    pub fn value(&self, i: usize) -> &[u8] {
        let offsets = self.offsets.as_slice();
        let offset = offsets[i];
        let offset_1 = offsets[i + 1];
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        &self.values.as_slice()[offset..offset + length]
    }

    /// Returns the element at index `i` as &str
    /// # Safety
    /// Assumes that the `i < self.len`.
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let offset = *self.offsets.as_ptr().add(i);
        let offset_1 = *self.offsets.as_ptr().add(i + 1);
        let length = (offset_1 - offset).to_usize().unwrap();
        let offset = offset.to_usize().unwrap();

        std::slice::from_raw_parts(self.values.as_ptr().add(offset), length)
    }

    #[inline]
    pub fn offsets(&self) -> &[O] {
        self.offsets.as_slice()
    }

    #[inline]
    pub fn offsets_buffer(&self) -> &Buffer<O> {
        &self.offsets
    }

    #[inline]
    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    #[inline]
    pub fn values_buffer(&self) -> &Buffer<u8> {
        &self.values
    }
}

impl<O: Offset> Array for BinaryArray<O> {
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

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl<O: Offset> std::fmt::Display for BinaryArray<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = |x: &[u8]| display_helper(x.iter().map(|x| Some(format!("{:b}", x)))).join(" ");
        let iter = self.iter().map(|x| x.map(a));
        let head = if O::is_large() {
            "LargeBinaryArray"
        } else {
            "BinaryArray"
        };
        display_fmt(iter, head, f, false)
    }
}

unsafe impl<O: Offset> ToFFI for BinaryArray<O> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.offsets.as_ptr() as *mut u8
                )),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
            ]
        }
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<O: Offset> FromFFI for BinaryArray<O> {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let expected = if O::is_large() {
            DataType::LargeBinary
        } else {
            DataType::Binary
        };
        assert_eq!(data_type, expected);

        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut offsets = unsafe { array.buffer::<O>(0)? };
        let values = unsafe { array.buffer::<u8>(1)? };

        if offset > 0 {
            offsets = offsets.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }

        Ok(Self {
            data_type,
            offsets,
            values,
            validity,
            offset: 0,
        })
    }
}

mod iterator;
pub use iterator::*;
mod from;
pub use from::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn basics() {
        let data = vec![Some(b"hello".to_vec()), None, Some(b"hello2".to_vec())];

        let array = BinaryArray::<i32>::from_iter(data);

        assert_eq!(array.value(0), b"hello");
        assert_eq!(array.value(1), b"");
        assert_eq!(array.value(2), b"hello2");
        assert_eq!(unsafe { array.value_unchecked(2) }, b"hello2");
        assert_eq!(array.values(), b"hellohello2");
        assert_eq!(array.offsets(), &[0, 5, 5, 11]);
        assert_eq!(array.nulls(), &Some(Bitmap::from((&[0b00000101], 3))));
        assert_eq!(array.is_valid(0), true);
        assert_eq!(array.is_valid(1), false);
        assert_eq!(array.is_valid(2), true);

        let array2 = BinaryArray::<i32>::from_data(
            array.offsets_buffer().clone(),
            array.values_buffer().clone(),
            array.nulls().clone(),
        );
        assert_eq!(array, array2);

        let array = array.slice(1, 2);
        assert_eq!(array.value(0), b"");
        assert_eq!(array.value(1), b"hello2");
        // note how this keeps everything: the offsets were sliced
        assert_eq!(array.values(), b"hellohello2");
        assert_eq!(array.offsets(), &[5, 5, 11]);
    }

    #[test]
    fn empty() {
        let array = BinaryArray::<i32>::new_empty();
        assert_eq!(array.values(), b"");
        assert_eq!(array.offsets(), &[0]);
        assert_eq!(array.nulls(), &None);
    }
}
