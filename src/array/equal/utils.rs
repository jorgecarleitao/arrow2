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

use crate::{array::Array, buffer::Bitmap};

// whether bits along the positions are equal
// `lhs_start`, `rhs_start` and `len` are _measured in bits_.
#[inline]
pub(super) fn equal_bits(
    lhs_values: &Bitmap,
    rhs_values: &Bitmap,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    // todo: safely iterate over both in one go
    (0..len).all(|i| lhs_values.get_bit(lhs_start + i) == rhs_values.get_bit(rhs_start + i))
}

#[inline]
pub(super) fn equal_nulls(
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_null_count = lhs_nulls
        .as_ref()
        .map(|x| x.null_count_range(lhs_start, len))
        .unwrap_or(0);
    let rhs_null_count = rhs_nulls
        .as_ref()
        .map(|x| x.null_count_range(rhs_start, len))
        .unwrap_or(0);

    if lhs_null_count > 0 || rhs_null_count > 0 {
        let lhs_values = lhs_nulls.as_ref().unwrap();
        let rhs_values = rhs_nulls.as_ref().unwrap();
        equal_bits(lhs_values, rhs_values, lhs_start, rhs_start, len)
    } else {
        true
    }
}

#[inline]
pub(super) fn base_equal(lhs: &dyn Array, rhs: &dyn Array) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len()
}

// whether the two memory regions are equal
#[inline]
pub(super) fn equal_len<T: PartialEq>(
    lhs_values: &[T],
    rhs_values: &[T],
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    lhs_values[lhs_start..(lhs_start + len)] == rhs_values[rhs_start..(rhs_start + len)]
}
