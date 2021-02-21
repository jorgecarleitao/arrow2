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

use crate::array::{list::iterator::ListIter, Array, IterableListArray};

use super::FixedSizeListArray;

impl IterableListArray for FixedSizeListArray {
    fn value(&self, i: usize) -> Box<dyn Array> {
        FixedSizeListArray::value(self, i)
    }
}

impl<'a> IntoIterator for &'a FixedSizeListArray {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ListIter<'a, FixedSizeListArray>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter::new(self)
    }
}

impl<'a> FixedSizeListArray {
    /// constructs a new iterator
    pub fn iter(&'a self) -> ListIter<'a, Self> {
        ListIter::new(&self)
    }
}
