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

//! This module contains two main structs: [Buffer] and [MutableBuffer]. A buffer represents
//! a contiguous memory region that can be shared via `offsets`.

use crate::ffi;

use super::{
    bytes::{Bytes, Deallocation},
    types::NativeType,
};

use std::sync::Arc;
use std::{convert::AsRef, usize};
use std::{fmt::Debug, iter::FromIterator};

use super::mutable::MutableBuffer;

/// Buffer represents a contiguous memory region that can be shared with other buffers and across
/// thread boundaries.
#[derive(Clone, PartialEq, Debug)]
pub struct Buffer<T: NativeType> {
    /// the internal byte buffer.
    data: Arc<Bytes<T>>,

    /// The offset into the buffer.
    offset: usize,

    // the length of the buffer. Given a region `data` of N bytes, [offset..offset+length] is visible
    // to this buffer.
    length: usize,
}

impl<T: NativeType> Buffer<T> {
    #[inline]
    pub fn new() -> Self {
        MutableBuffer::new().into()
    }

    /// Auxiliary method to create a new Buffer
    #[inline]
    pub fn from_bytes(bytes: Bytes<T>) -> Self {
        let length = bytes.len();
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
            length,
        }
    }

    /// Returns the number of bytes in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the byte slice stored in this buffer
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.data[self.offset..self.offset + self.length]
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `offset` is larger than `len`.
    #[inline]
    pub fn slice(mut self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        self.offset += offset;
        self.length = length;
        self
    }

    /// Returns a pointer to the start of this buffer.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        unsafe { self.data.ptr().as_ptr().add(self.offset) }
    }
}

impl<T: NativeType> Buffer<T> {
    #[inline]
    pub unsafe fn from_trusted_len_iter<I: Iterator<Item = T>>(iterator: I) -> Self {
        MutableBuffer::from_trusted_len_iter(iterator).into()
    }
}

/// Creating a `Buffer` instance by copying the memory from a `AsRef<[u8]>` into a newly
/// allocated memory region.
impl<T: NativeType, U: AsRef<[T]>> From<U> for Buffer<T> {
    #[inline]
    fn from(p: U) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len();
        let mut buffer = MutableBuffer::with_capacity(len);
        buffer.extend_from_slice(slice);
        buffer.into()
    }
}

impl<T: NativeType> FromIterator<T> for Buffer<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        MutableBuffer::from_iter(iter).into()
    }
}

impl<T: NativeType> Buffer<T> {
    /// Creates a buffer from an existing memory region (must already be byte-aligned), this
    /// `Buffer` **does not** free this piece of memory when dropped.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `data` - An [ffi::FFI_ArrowArray] with the data
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes and that the foreign deallocator frees the region.
    #[inline]
    pub unsafe fn from_unowned(
        ptr: std::ptr::NonNull<u8>,
        length: usize,
        data: Arc<ffi::FFI_ArrowArray>,
    ) -> Self {
        // todo: make all kinds of assertions here wrt to alignment, len, etc.
        let ptr = ptr.as_ptr() as *mut T;
        let ptr =
            std::ptr::NonNull::<T>::new(ptr).expect("Can't cast pointer from FFI: it is null");
        let bytes = Bytes::new(ptr, length, Deallocation::Foreign(data));
        Self {
            data: Arc::new(bytes),
            offset: 0,
            length,
        }
    }
}
