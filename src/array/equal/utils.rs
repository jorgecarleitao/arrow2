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
    array::{Array, ListArray, Offset},
    buffer::{Bitmap, MutableBitmap},
    datatypes::DataType,
};

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
pub(super) fn equal_validity(
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_null_count = lhs_validity
        .as_ref()
        .map(|x| x.null_count_range(lhs_start, len))
        .unwrap_or(0);
    let rhs_null_count = rhs_validity
        .as_ref()
        .map(|x| x.null_count_range(rhs_start, len))
        .unwrap_or(0);

    if lhs_null_count > 0 || rhs_null_count > 0 {
        let lhs_values = lhs_validity.as_ref().unwrap();
        let rhs_values = rhs_validity.as_ref().unwrap();
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

#[inline]
pub(super) fn count_validity(validity: &Option<Bitmap>, offset: usize, length: usize) -> usize {
    validity
        .as_ref()
        .map(|x| x.null_count_range(offset, length))
        .unwrap_or(0)
}

// Calculate a list child's logical bitmap
// `[[1, None, 3], None, [None]]`
// offsets = [0, 3, 3, 4]
// parent validity = [1, 0, 1]
// child validity = [1, 0, 1, 0]
// logical_list_bitmap = [1, 0, 1, 0]
#[inline]
fn logical_list_bitmap<O: Offset>(
    offsets: &[O],
    parent_bitmap: &Option<Bitmap>,
    child_bitmap: &Option<Bitmap>,
) -> Option<Bitmap> {
    let first_offset = offsets.first().unwrap().to_usize().unwrap();
    let last_offset = offsets.get(offsets.len() - 1).unwrap().to_usize().unwrap();
    let length = last_offset - first_offset;

    match (parent_bitmap, child_bitmap) {
        (Some(parent_bitmap), Some(child_bitmap)) => {
            let mut buffer = MutableBitmap::with_capacity(length);
            offsets.windows(2).enumerate().for_each(|(index, window)| {
                let start = window[0].to_usize().unwrap();
                let end = window[1].to_usize().unwrap();
                let mask = parent_bitmap.get_bit(index);
                (start..end).for_each(|child_index| {
                    let is_set = mask && child_bitmap.get_bit(child_index);
                    buffer.push(is_set);
                });
            });
            Some(buffer.into())
        }
        (None, Some(child_bitmap)) => {
            let mut buffer = MutableBitmap::with_capacity(length);
            offsets.windows(2).for_each(|window| {
                let start = window[0].to_usize().unwrap();
                let end = window[1].to_usize().unwrap();
                (start..end).for_each(|child_index| {
                    buffer.push(child_bitmap.get_bit(child_index));
                });
            });
            Some(buffer.into())
        }
        (Some(parent_bitmap), None) => {
            let mut buffer = MutableBitmap::with_capacity(length);
            offsets.windows(2).enumerate().for_each(|(index, window)| {
                let start = window[0].to_usize().unwrap();
                let end = window[1].to_usize().unwrap();
                let mask = parent_bitmap.get_bit(index);
                (start..end).for_each(|_| {
                    buffer.push(mask);
                });
            });
            Some(buffer.into())
        }
        (None, None) => None,
    }
}

pub(super) fn child_logical_null_buffer(
    parent: &dyn Array,
    logical_null_buffer: &Option<Bitmap>,
    child: &dyn Array,
) -> Option<Bitmap> {
    let parent_bitmap = logical_null_buffer;
    let self_null_bitmap = child.validity();
    match parent.data_type() {
        DataType::List(_) => {
            let parent = parent.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            logical_list_bitmap(parent.offsets(), parent_bitmap, self_null_bitmap)
        }
        DataType::LargeList(_) => {
            let parent = parent.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            logical_list_bitmap(parent.offsets(), parent_bitmap, self_null_bitmap)
        }
        DataType::FixedSizeList(_, len) => {
            let offsets = (0..=parent.len())
                .map(|x| (x as i32) * *len)
                .collect::<Vec<i32>>();
            logical_list_bitmap(&offsets, parent_bitmap, self_null_bitmap)
        }
        DataType::Struct(_) => match (parent_bitmap, self_null_bitmap) {
            (None, None) => None,
            (Some(p), None) => Some(p.clone()),
            (None, Some(c)) => Some(c.clone()),
            (Some(p), Some(c)) => Some(p & c),
        },
        data_type => panic!("Data type {:?} is not a supported nested type", data_type),
    }
}
