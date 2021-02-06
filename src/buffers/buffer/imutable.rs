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

use super::super::{
    bytes::{Bytes, Deallocation},
    types::NativeType,
};

use std::fmt::Debug;
use std::ptr::NonNull;
use std::sync::Arc;
use std::{convert::AsRef, usize};

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
    pub fn new() -> Self {
        MutableBuffer::new().into()
    }

    /// Auxiliary method to create a new Buffer
    pub(super) unsafe fn build_with_arguments(
        ptr: NonNull<T>,
        len: usize,
        deallocation: Deallocation,
    ) -> Self {
        let bytes = Bytes::new(ptr, len, deallocation);
        Buffer {
            data: Arc::new(bytes),
            offset: 0,
            length: len,
        }
    }

    /// Returns the number of bytes in the buffer
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the byte slice stored in this buffer
    pub fn as_slice(&self) -> &[T] {
        &self.data[self.offset..self.offset + self.length]
    }

    /// Returns a new [Buffer] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `offset` is larger than `len`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        Self {
            data: self.data.clone(),
            offset: self.offset + offset,
            length,
        }
    }

    /// Returns a pointer to the start of this buffer.
    ///
    /// Note that this should be used cautiously, and the returned pointer should not be
    /// stored anywhere, to avoid dangling pointers.
    pub fn as_ptr(&self) -> *const T {
        unsafe { self.data.ptr().as_ptr().add(self.offset) }
    }
}

/// Creating a `Buffer` instance by copying the memory from a `AsRef<[u8]>` into a newly
/// allocated memory region.
impl<T: NativeType, U: AsRef<[T]>> From<U> for Buffer<T> {
    fn from(p: U) -> Self {
        // allocate aligned memory buffer
        let slice = p.as_ref();
        let len = slice.len();
        let mut buffer = MutableBuffer::with_capacity(len);
        buffer.extend_from_slice(slice);
        buffer.into()
    }
}

/// Creating a `Buffer` instance by storing the boolean values into the buffer
impl std::iter::FromIterator<bool> for Buffer<u8> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = bool>,
    {
        MutableBuffer::from_iter(iter).into()
    }
}
