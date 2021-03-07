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
    buffer::{MutableBitmap, MutableBuffer},
};

pub(super) fn extend_offsets<T: Offset>(
    buffer: &mut MutableBuffer<T>,
    last_offset: &mut T,
    offsets: &[T],
) {
    buffer.reserve(offsets.len());
    offsets.windows(2).for_each(|offsets| {
        // compute the new offset
        let length = offsets[1] - offsets[0];
        *last_offset += length;
        buffer.push(*last_offset);
    });
}

pub(super) type ExtendNullBits<'a> = Box<dyn Fn(&mut MutableBitmap, usize, usize) + 'a>;

pub(super) fn build_extend_null_bits(array: &dyn Array, use_validity: bool) -> ExtendNullBits {
    if let Some(bitmap) = array.validity() {
        Box::new(move |validity, start, len| {
            validity.reserve(len);
            unsafe {
                (start..start + len).for_each(|i| validity.push_unchecked(bitmap.get_bit(i)))
            };
        })
    } else if use_validity {
        Box::new(|validity, _, len| {
            validity.reserve(len);
            (0..len).for_each(|_| {
                unsafe { validity.push_unchecked(true) };
            });
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

#[inline]
pub(super) fn extend_offset_values<O: Offset>(
    buffer: &mut MutableBuffer<u8>,
    offsets: &[O],
    values: &[u8],
    start: usize,
    len: usize,
) {
    let start_values = offsets[start].to_usize().unwrap();
    let end_values = offsets[start + len].to_usize().unwrap();
    let new_values = &values[start_values..end_values];
    buffer.extend_from_slice(new_values);
}
