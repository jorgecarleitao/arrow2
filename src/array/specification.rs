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

use std::convert::TryFrom;

use num::Num;

use crate::buffer::{Buffer, NativeType};

/// Trait uses to distinguish types whose offset sizes support multiple sizes.
/// This trait is only implemented for i32 and i64, which are the two sizes currently
/// declared in arrow specification.
/// # Safety
/// Do not implement.
pub unsafe trait Offset: NativeType + Num + Ord + std::ops::AddAssign {
    fn is_large() -> bool;

    fn to_usize(&self) -> Option<usize>;

    fn from_usize(value: usize) -> Option<Self>;
}

unsafe impl Offset for i32 {
    #[inline]
    fn is_large() -> bool {
        false
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        Some(*self as usize)
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Self::try_from(value).ok()
    }
}

unsafe impl Offset for i64 {
    #[inline]
    fn is_large() -> bool {
        true
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        usize::try_from(*self).ok()
    }

    #[inline]
    fn from_usize(value: usize) -> Option<Self> {
        Some(value as i64)
    }
}

#[inline]
pub fn check_offsets<O: Offset>(offsets: &Buffer<O>, values_len: usize) -> usize {
    assert!(
        offsets.len() >= 1,
        "The length of the offset buffer must be larger than 1"
    );
    let len = offsets.len() - 1;

    let offsets = offsets.as_slice();

    let last_offset = offsets[len];
    let last_offset = last_offset
        .to_usize()
        .expect("The last offset of the array is larger than usize::MAX");

    assert_eq!(
        values_len, last_offset,
        "The length of the values must be equal to the last offset value"
    );
    len
}

#[inline]
pub fn check_offsets_and_utf8<O: Offset>(offsets: &Buffer<O>, values: &Buffer<u8>) -> usize {
    let len = check_offsets(offsets, values.len());
    offsets.as_slice().windows(2).for_each(|window| {
        let start = window[0]
            .to_usize()
            .expect("The last offset of the array is larger than usize::MAX");
        let end = window[1]
            .to_usize()
            .expect("The last offset of the array is larger than usize::MAX");
        assert!(end <= values.len());
        let slice = unsafe { std::slice::from_raw_parts(values.as_ptr().add(start), end - start) };
        std::str::from_utf8(slice).expect("A non-utf8 string was passed.");
    });
    len
}
