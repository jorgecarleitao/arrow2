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
    array::{DictionaryArray, DictionaryKey},
    buffer::Bitmap,
};

use super::equal as _equal;
use super::utils::count_nulls;

pub(super) fn equal<K: DictionaryKey>(
    lhs: &DictionaryArray<K>,
    rhs: &DictionaryArray<K>,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_keys = lhs.keys();
    let rhs_keys = rhs.keys();
    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        lhs_keys
            .iter()
            .zip(rhs_keys.iter())
            .all(|(lhs, rhs)| match (lhs, rhs) {
                (None, None) => true,
                (Some(_), None) => false,
                (None, Some(_)) => false,
                (Some(l), Some(r)) => {
                    let lhs = lhs_values.slice(l.to_usize().unwrap(), 1);
                    let rhs = rhs_values.slice(r.to_usize().unwrap(), 1);
                    _equal(lhs.as_ref(), rhs.as_ref())
                }
            })
    } else {
        let lhs_bitmap = lhs_nulls.as_ref().unwrap();
        let rhs_bitmap = rhs_nulls.as_ref().unwrap();
        let lhs_bitmap_iter = lhs_bitmap.iter();
        let rhs_bitmap_iter = rhs_bitmap.iter();
        let keys_iter = lhs_keys.iter().zip(rhs_keys.iter());
        let bitmap_iter = lhs_bitmap_iter.zip(rhs_bitmap_iter);
        let mut iter = keys_iter.zip(bitmap_iter).skip(lhs_start).take(len);
        iter.all(|((lhs, rhs), (bit1, bit2))| {
            if !bit1 && !bit2 {
                return true;
            };
            if bit1 != bit2 {
                return false;
            };
            match (lhs, rhs) {
                (None, None) => true,
                (Some(_), None) => false,
                (None, Some(_)) => false,
                (Some(l), Some(r)) => {
                    let lhs = lhs_values.slice(l.to_usize().unwrap(), 1);
                    let rhs = lhs_values.slice(r.to_usize().unwrap(), 1);
                    _equal(lhs.as_ref(), rhs.as_ref())
                }
            }
        })
    }
}
