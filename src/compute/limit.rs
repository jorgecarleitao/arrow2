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

use crate::array::Array;

/// Returns the array, taking only the number of elements specified
///
/// Limit performs a zero-copy slice of the array, and is a convenience method on slice
/// where:
/// * it performs a bounds-check on the array
/// * it slices from offset 0
pub fn limit(array: &dyn Array, num_elements: usize) -> Box<dyn Array> {
    let lim = num_elements.min(array.len());
    array.slice(0, lim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;

    #[test]
    fn test_limit_array() {
        let a = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
        let b = limit(&a, 3);
        let c = b.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from_slice(&[5, 6, 7]);
        assert_eq!(&expected, c);
    }
}
