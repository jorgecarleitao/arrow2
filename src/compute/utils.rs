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
    array::{Array, BooleanArray, Offset, Utf8Array},
    bitmap::Bitmap,
};

pub fn combine_validities(lhs: &Option<Bitmap>, rhs: &Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
    }
}

pub fn unary_utf8_boolean<O: Offset, F: Fn(&str) -> bool>(
    values: &Utf8Array<O>,
    op: F,
) -> BooleanArray {
    let validity = values.validity().clone();

    let iterator = values.iter().map(|value| {
        if value.is_none() {
            return false;
        };
        op(value.unwrap())
    });
    let values = Bitmap::from_trusted_len_iter(iterator);
    BooleanArray::from_data(values, validity)
}
