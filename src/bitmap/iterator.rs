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

use super::Bitmap;

/// Iterator of Option<T> from an iterator and validity.
pub struct BitmapIter<'a> {
    iter: std::slice::Iter<'a, u8>,
    current_byte: &'a u8,
    len: usize,
    index: usize,
    mask: u8,
}

impl<'a> BitmapIter<'a> {
    #[inline]
    pub fn new(bitmap: &'a Bitmap) -> Self {
        let offset = bitmap.offset();
        let len = bitmap.len();
        let bytes = &bitmap.bytes()[offset / 8..];

        let mut iter = bytes.iter();

        let current_byte = iter.next().unwrap_or(&0);

        Self {
            iter,
            mask: 1u8.rotate_left(offset as u32),
            len,
            index: 0,
            current_byte,
        }
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // easily predictable in branching
        if self.index == self.len {
            return None;
        } else {
            self.index += 1;
        }
        let value = self.current_byte & self.mask != 0;
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            // reached a new byte => try to fetch it from the iterator
            match self.iter.next() {
                Some(v) => self.current_byte = v,
                None => return None,
            }
        }
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.index, Some(self.len - self.index))
    }
}
