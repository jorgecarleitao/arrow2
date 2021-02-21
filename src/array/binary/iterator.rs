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

use crate::array::IterableBinaryArray;
use crate::array::Offset;

use super::BinaryArray;

impl<O: Offset> IterableBinaryArray for BinaryArray<O> {
    unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        BinaryArray::<O>::value_unchecked(self, i)
    }
}

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = BinaryIter<'a, BinaryArray<O>>;

    fn into_iter(self) -> Self::IntoIter {
        BinaryIter::new(self)
    }
}

impl<'a, O: Offset> BinaryArray<O> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> BinaryIter<'a, Self> {
        BinaryIter::new(&self)
    }
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct BinaryIter<'a, A>
where
    A: IterableBinaryArray,
{
    array: &'a A,
    i: usize,
    len: usize,
}

impl<'a, A: IterableBinaryArray> BinaryIter<'a, A> {
    /// create a new iterator
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, A: IterableBinaryArray> std::iter::Iterator for BinaryIter<'a, A> {
    type Item = Option<&'a [u8]>;

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
impl<'a, A: IterableBinaryArray> std::iter::ExactSizeIterator for BinaryIter<'a, A> {}
