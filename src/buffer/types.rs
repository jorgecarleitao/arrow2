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

use crate::datatypes::{DataType, IntervalUnit};

/// Trait declaring any type that can be allocated, serialized and deserialized by this crate.
/// All data-heavy memory operations are implemented for this trait alone.
/// # Safety
/// Do not implement.
pub unsafe trait NativeType:
    Sized + Copy + std::fmt::Debug + std::fmt::Display + PartialEq + Default + Sized + 'static
{
    type Bytes: AsRef<[u8]>;

    fn is_valid(data_type: &DataType) -> bool;

    fn to_le_bytes(&self) -> Self::Bytes;
}

unsafe impl NativeType for u8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt8
    }
}

unsafe impl NativeType for u16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt16
    }
}

unsafe impl NativeType for u32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt32
    }
}

unsafe impl NativeType for u64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt64
    }
}

unsafe impl NativeType for i8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int8
    }
}

unsafe impl NativeType for i16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int16
    }
}

unsafe impl NativeType for i32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

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

unsafe impl NativeType for i64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

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

unsafe impl NativeType for i128 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Decimal(_, _) => true,
            _ => false,
        }
    }
}

unsafe impl NativeType for f32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float32
    }
}

unsafe impl NativeType for f64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float64
    }
}

/// The in-memory representation of the DayMillisecond variant of arrow's "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq)]
#[allow(non_camel_case_types)]
pub struct days_ms([i32; 2]);

impl std::fmt::Display for days_ms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}d {}ms", self.days(), self.milliseconds())
    }
}

unsafe impl NativeType for days_ms {
    type Bytes = Vec<u8>;
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        // todo: find a way of avoiding this allocation
        [self.0[0].to_le_bytes(), self.0[1].to_le_bytes()].concat()
    }

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
