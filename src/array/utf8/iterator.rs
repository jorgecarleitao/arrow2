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

use crate::array::{Array, Offset};

use super::Utf8Array;

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
