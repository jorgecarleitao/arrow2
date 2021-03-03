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
use std::cmp::{Ord, Ordering};

use crate::datatypes::{DataType, IntervalUnit};

pub unsafe trait Relation {
    fn is_valid(data_type: &DataType) -> bool;
}

/// Trait declaring any type that can be allocated, serialized and deserialized by this crate.
/// All data-heavy memory operations are implemented for this trait alone.
/// # Safety
/// Do not implement.
pub unsafe trait NativeType:
    Relation
    + Sized
    + Copy
    + std::fmt::Debug
    + std::fmt::Display
    + PartialEq
    + Default
    + Sized
    + 'static
{
    type Bytes: AsRef<[u8]>;

    fn to_le_bytes(&self) -> Self::Bytes;

    fn to_be_bytes(&self) -> Self::Bytes;

    fn from_be_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! native {
    ($type:ty) => {
        unsafe impl NativeType for $type {
            type Bytes = [u8; std::mem::size_of::<Self>()];
            #[inline]
            fn to_le_bytes(&self) -> Self::Bytes {
                Self::to_le_bytes(*self)
            }

            #[inline]
            fn to_be_bytes(&self) -> Self::Bytes {
                Self::to_be_bytes(*self)
            }

            #[inline]
            fn from_be_bytes(bytes: Self::Bytes) -> Self {
                Self::from_be_bytes(bytes)
            }
        }
    };
}

native!(u8);
native!(u16);
native!(u32);
native!(u64);
native!(i8);
native!(i16);
native!(i32);
native!(i64);
native!(i128);
native!(f32);
native!(f64);

unsafe impl Relation for u8 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt8
    }
}

unsafe impl Relation for u16 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt16
    }
}

unsafe impl Relation for u32 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt32
    }
}

unsafe impl Relation for u64 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt64
    }
}

unsafe impl Relation for i8 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int8
    }
}

unsafe impl Relation for i16 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int16
    }
}

unsafe impl Relation for i32 {
    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => true,
            _ => false,
        }
    }
}

unsafe impl Relation for i64 {
    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => true,
            _ => false,
        }
    }
}

unsafe impl Relation for i128 {
    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Decimal(_, _) => true,
            _ => false,
        }
    }
}

unsafe impl Relation for f32 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float32
    }
}

unsafe impl Relation for f64 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float64
    }
}

/// The in-memory representation of the DayMillisecond variant of arrow's "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub struct days_ms([i32; 2]);

impl std::fmt::Display for days_ms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}d {}ms", self.days(), self.milliseconds())
    }
}

unsafe impl NativeType for days_ms {
    type Bytes = [u8; 8];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        let days = self.0[0].to_le_bytes();
        let ms = self.0[1].to_le_bytes();
        let mut result = [0; 8];
        result[0] = days[0];
        result[1] = days[1];
        result[2] = days[2];
        result[3] = days[3];
        result[4] = ms[0];
        result[5] = ms[1];
        result[6] = ms[2];
        result[7] = ms[3];
        result
    }

    #[inline]
    fn to_be_bytes(&self) -> Self::Bytes {
        let days = self.0[0].to_be_bytes();
        let ms = self.0[1].to_be_bytes();
        let mut result = [0; 8];
        result[0] = days[0];
        result[1] = days[1];
        result[2] = days[2];
        result[3] = days[3];
        result[4] = ms[0];
        result[5] = ms[1];
        result[6] = ms[2];
        result[7] = ms[3];
        result
    }

    #[inline]
    fn from_be_bytes(bytes: Self::Bytes) -> Self {
        let mut days = [0; 4];
        days[0] = bytes[0];
        days[1] = bytes[1];
        days[2] = bytes[2];
        days[3] = bytes[3];
        let mut ms = [0; 4];
        ms[0] = bytes[4];
        ms[1] = bytes[5];
        ms[2] = bytes[6];
        ms[3] = bytes[7];
        Self([i32::from_be_bytes(days), i32::from_be_bytes(ms)])
    }
}

unsafe impl Relation for days_ms {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Interval(IntervalUnit::DayTime)
    }
}

impl days_ms {
    #[inline]
    pub fn new(days: i32, milliseconds: i32) -> Self {
        Self([days, milliseconds])
    }

    #[inline]
    pub fn days(&self) -> i32 {
        self.0[0]
    }

    #[inline]
    pub fn milliseconds(&self) -> i32 {
        self.0[1]
    }
}

impl Ord for days_ms {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.days().cmp(&other.days()) {
            Ordering::Equal => self.milliseconds().cmp(&other.milliseconds()),
            other => other,
        }
    }
}

impl PartialOrd for days_ms {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
