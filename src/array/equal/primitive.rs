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

use crate::{array::primitive::PrimitiveArray, bitmap::Bitmap, types::NativeType};

use super::utils::{count_validity, equal_len};

pub(super) fn equal<T: NativeType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_values = &lhs.values();
    let rhs_values = &rhs.values();

    let lhs_null_count = count_validity(lhs_validity, lhs_start, len);
    let rhs_null_count = count_validity(rhs_validity, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        // without nulls, we just need to compare slices
        equal_len(lhs_values, rhs_values, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_bitmap = lhs_validity.as_ref().unwrap();
        let rhs_bitmap = rhs_validity.as_ref().unwrap();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_len(lhs_values, rhs_values, lhs_pos, rhs_pos, 1)
        })
    }
}
