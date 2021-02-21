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
    array::Array,
    buffer::{Bitmap, NativeType},
};

use super::PrimitiveArray;

/// an iterator that returns Some(T) or None, that can be used on any PrimitiveArray
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct PrimitiveIter<'a, T: NativeType> {
    values: &'a [T],
    validity: &'a Option<Bitmap>,
    current: usize,
    current_end: usize,
}

impl<'a, T: NativeType> PrimitiveIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a PrimitiveArray<T>) -> Self {
        PrimitiveIter::<T> {
            values: array.values(),
            validity: &array.validity,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a, T: NativeType> std::iter::Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if !self
            .validity
            .as_ref()
            .map(|x| x.get_bit(self.current))
            .unwrap_or(true)
        {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            Some(Some(self.values[old]))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.values.len() - self.current,
            Some(self.values.len() - self.current),
        )
    }
}

impl<'a, T: NativeType> IntoIterator for &'a PrimitiveArray<T> {
    type Item = Option<T>;
    type IntoIter = PrimitiveIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        PrimitiveIter::<'a, T>::new(self)
    }
}

impl<'a, T: NativeType> PrimitiveArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> PrimitiveIter<'a, T> {
        PrimitiveIter::<'a, T>::new(&self)
    }
}
