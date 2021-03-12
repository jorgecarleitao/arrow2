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
    array::{Array, Offset},
    bits::{zip_validity, ZipValidity},
};

use super::BinaryArray;

/// # Safety
/// This iterator is `TrustedLen`
pub struct BinaryValueIter<'a, O: Offset> {
    array: &'a BinaryArray<O>,
    index: usize,
}

impl<'a, O: Offset> BinaryValueIter<'a, O> {
    #[inline]
    pub fn new(array: &'a BinaryArray<O>) -> Self {
        Self { array, index: 0 }
    }
}

impl<'a, O: Offset> Iterator for BinaryValueIter<'a, O> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            return None;
        } else {
            self.index += 1;
        }
        Some(unsafe { self.array.value_unchecked(self.index - 1) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.index,
            Some(self.array.len() - self.index),
        )
    }
}

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<'a, &'a [u8], BinaryValueIter<'a, O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> BinaryArray<O> {
    /// Returns an iterator of `Option<&[u8]>`
    pub fn iter(&'a self) -> ZipValidity<'a, &'a [u8], BinaryValueIter<'a, O>> {
        zip_validity(BinaryValueIter::new(self), &self.validity)
    }

    /// Returns an iterator of `&[u8]`
    pub fn values_iter(&'a self) -> BinaryValueIter<'a, O> {
        BinaryValueIter::new(self)
    }
}
