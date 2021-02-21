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

use crate::array::Offset;
use crate::array::{Array, IterableListArray};

use super::ListArray;

impl<O: Offset> IterableListArray for ListArray<O> {
    fn value(&self, i: usize) -> Box<dyn Array> {
        ListArray::<O>::value(self, i)
    }
}

impl<'a, O: Offset> IntoIterator for &'a ListArray<O> {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ListIter<'a, ListArray<O>>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter::new(self)
    }
}

impl<'a, O: Offset> ListArray<O> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> ListIter<'a, Self> {
        ListIter::new(&self)
    }
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct ListIter<'a, A>
where
    A: IterableListArray,
{
    array: &'a A,
    i: usize,
    len: usize,
}

impl<'a, A: IterableListArray> ListIter<'a, A> {
    /// create a new iterator
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, A: IterableListArray> std::iter::Iterator for ListIter<'a, A> {
    type Item = Option<Box<dyn Array>>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(self.array.value(i)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, A: IterableListArray> std::iter::ExactSizeIterator for ListIter<'a, A> {}
