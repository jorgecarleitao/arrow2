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

use std::sync::Arc;

use crate::array::{Array, NullArray};

use super::Growable;

/// A growable PrimitiveArray
pub struct GrowableNull {
    length: usize,
}

impl GrowableNull {
    pub fn new() -> Self {
        Self { length: 0 }
    }
}

impl<'a> Growable<'a> for GrowableNull {
    fn extend(&mut self, _: usize, _: usize, len: usize) {
        self.length += len;
    }

    fn extend_nulls(&mut self, additional: usize) {
        self.length += additional;
    }

    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(NullArray::from_data(self.length))
    }
}

impl Into<NullArray> for GrowableNull {
    fn into(self) -> NullArray {
        NullArray::from_data(self.length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let mut mutable = GrowableNull::new();

        mutable.extend(0, 1, 2);
        mutable.extend(1, 0, 1);

        let result: NullArray = mutable.into();

        let expected = NullArray::from_data(3);
        assert_eq!(result, expected);
    }
}
