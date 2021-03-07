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

use std::{
    fmt::Binary,
    ops::{BitAnd, BitAndAssign, BitOr, Not, Shl, ShlAssign, ShrAssign},
};

/// Something that can be use as a chunk of bits.
/// Currently implemented for `u8` and `u64`
/// # Safety
/// Do not implement.
pub unsafe trait BitChunk:
    Sized
    + Copy
    + std::fmt::Debug
    + Binary
    + BitAnd<Output = Self>
    + ShlAssign
    + Not<Output = Self>
    + ShrAssign<u32>
    + ShlAssign<u32>
    + Shl<u32, Output = Self>
    + Eq
    + BitAndAssign
    + BitOr<Output = Self>
{
    fn one() -> Self;
    fn zero() -> Self;
    fn to_le(self) -> Self;
}

unsafe impl BitChunk for u8 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}

unsafe impl BitChunk for u64 {
    #[inline(always)]
    fn zero() -> Self {
        0
    }

    #[inline(always)]
    fn to_le(self) -> Self {
        self.to_le()
    }

    #[inline(always)]
    fn one() -> Self {
        1
    }
}
