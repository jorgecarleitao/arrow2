//! Traits and implementations to handle _all types_ used in this crate.
//!
//! Most physical types used in this crate are native Rust types, like `i32`.
//! The most important trait is [`NativeType`], the generic trait of [`crate::array::PrimitiveArray`].
//!
//! Another important trait is [`BitChunk`], describing types that can be used to
//! represent chunks of bits (e.g. `u8`, `u16`), and [`BitChunkIter`], that can be used to
//! iterate over bitmaps in [`BitChunk`]s.
//! Finally, this module also contains traits used to compile code optimized for SIMD instructions at [`mod@simd`].
use std::{convert::TryFrom, ops::Neg};

mod bit_chunk;
pub use bit_chunk::{BitChunk, BitChunkIter};
mod index;
pub mod simd;
pub use index::*;

use crate::datatypes::{DataType, IntervalUnit, PhysicalType, PrimitiveType};

/// Trait denoting anything that has a natural logical [`DataType`].
/// For example, [`DataType::Int32`] for `i32`.
pub trait NaturalDataType {
    /// The natural [`DataType`].
    const DATA_TYPE: DataType;
}

mod private {
    pub trait Sealed {}

    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
    impl Sealed for i8 {}
    impl Sealed for i16 {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
    impl Sealed for i128 {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
    impl Sealed for super::days_ms {}
    impl Sealed for super::months_days_ns {}
}

/// describes whether a [`DataType`] is valid.
pub trait Relation: private::Sealed {
    /// Whether `data_type` is a valid [`DataType`].
    fn is_valid(data_type: &DataType) -> bool;
}

macro_rules! create_relation {
    ($native_ty:ty, $physical_ty:expr) => {
        impl Relation for $native_ty {
            #[inline]
            fn is_valid(data_type: &DataType) -> bool {
                data_type.to_physical_type() == $physical_ty
            }
        }
    };
}

macro_rules! natural_type {
    ($type:ty, $data_type:expr) => {
        impl NaturalDataType for $type {
            const DATA_TYPE: DataType = $data_type;
        }
    };
}

/// Sealed trait that implemented by all types that can be allocated,
/// serialized and deserialized by this crate.
/// All O(N) in-memory allocations are implemented for this trait alone.
pub trait NativeType:
    Relation
    + NaturalDataType
    + Send
    + Sync
    + Sized
    + Copy
    + std::fmt::Debug
    + std::fmt::Display
    + PartialEq
    + Default
    + 'static
{
    /// Type denoting its representation as bytes.
    /// This must be `[u8; N]` where `N = size_of::<T>`.
    type Bytes: AsRef<[u8]>
        + std::ops::Index<usize, Output = u8>
        + std::ops::IndexMut<usize, Output = u8>
        + for<'a> TryFrom<&'a [u8]>
        + std::fmt::Debug;

    /// To bytes in little endian
    fn to_le_bytes(&self) -> Self::Bytes;

    /// To bytes in native endian
    fn to_ne_bytes(&self) -> Self::Bytes;

    /// To bytes in big endian
    fn to_be_bytes(&self) -> Self::Bytes;

    /// From bytes in big endian
    fn from_be_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! native {
    ($type:ty) => {
        impl NativeType for $type {
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
            fn to_ne_bytes(&self) -> Self::Bytes {
                Self::to_ne_bytes(*self)
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

natural_type!(u8, DataType::UInt8);
natural_type!(u16, DataType::UInt16);
natural_type!(u32, DataType::UInt32);
natural_type!(u64, DataType::UInt64);
natural_type!(i8, DataType::Int8);
natural_type!(i16, DataType::Int16);
natural_type!(i32, DataType::Int32);
natural_type!(i64, DataType::Int64);
natural_type!(f32, DataType::Float32);
natural_type!(f64, DataType::Float64);
natural_type!(days_ms, DataType::Interval(IntervalUnit::DayTime));
natural_type!(
    months_days_ns,
    DataType::Interval(IntervalUnit::MonthDayNano)
);
natural_type!(i128, DataType::Decimal(32, 32)); // users should set the decimal when creating an array

create_relation!(u8, PhysicalType::Primitive(PrimitiveType::UInt8));
create_relation!(u16, PhysicalType::Primitive(PrimitiveType::UInt16));
create_relation!(u32, PhysicalType::Primitive(PrimitiveType::UInt32));
create_relation!(u64, PhysicalType::Primitive(PrimitiveType::UInt64));
create_relation!(i8, PhysicalType::Primitive(PrimitiveType::Int8));
create_relation!(i16, PhysicalType::Primitive(PrimitiveType::Int16));
create_relation!(i32, PhysicalType::Primitive(PrimitiveType::Int32));
create_relation!(i64, PhysicalType::Primitive(PrimitiveType::Int64));
create_relation!(i128, PhysicalType::Primitive(PrimitiveType::Int128));
create_relation!(f32, PhysicalType::Primitive(PrimitiveType::Float32));
create_relation!(f64, PhysicalType::Primitive(PrimitiveType::Float64));

/// The in-memory representation of the DayMillisecond variant of arrow's "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub struct days_ms([i32; 2]);

impl std::fmt::Display for days_ms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}d {}ms", self.days(), self.milliseconds())
    }
}

impl NativeType for days_ms {
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
    fn to_ne_bytes(&self) -> Self::Bytes {
        let days = self.0[0].to_ne_bytes();
        let ms = self.0[1].to_ne_bytes();
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

create_relation!(days_ms, PhysicalType::Primitive(PrimitiveType::DaysMs));

impl days_ms {
    /// A new [`days_ms`].
    #[inline]
    pub fn new(days: i32, milliseconds: i32) -> Self {
        Self([days, milliseconds])
    }

    /// The number of days
    #[inline]
    pub fn days(&self) -> i32 {
        self.0[0]
    }

    /// The number of milliseconds
    #[inline]
    pub fn milliseconds(&self) -> i32 {
        self.0[1]
    }
}

/// The in-memory representation of the MonthDayNano variant of the "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct months_days_ns(i32, i32, i64);

impl std::fmt::Display for months_days_ns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}m {}d {}ns", self.months(), self.days(), self.ns())
    }
}

impl NativeType for months_days_ns {
    type Bytes = [u8; 16];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        let months = self.months().to_le_bytes();
        let days = self.days().to_le_bytes();
        let ns = self.ns().to_le_bytes();
        let mut result = [0; 16];
        result[0] = months[0];
        result[1] = months[1];
        result[2] = months[2];
        result[3] = months[3];
        result[4] = days[0];
        result[5] = days[1];
        result[6] = days[2];
        result[7] = days[3];
        (0..8).for_each(|i| {
            result[8 + i] = ns[i];
        });
        result
    }

    #[inline]
    fn to_ne_bytes(&self) -> Self::Bytes {
        let months = self.months().to_ne_bytes();
        let days = self.days().to_ne_bytes();
        let ns = self.ns().to_ne_bytes();
        let mut result = [0; 16];
        result[0] = months[0];
        result[1] = months[1];
        result[2] = months[2];
        result[3] = months[3];
        result[4] = days[0];
        result[5] = days[1];
        result[6] = days[2];
        result[7] = days[3];
        (0..8).for_each(|i| {
            result[8 + i] = ns[i];
        });
        result
    }

    #[inline]
    fn to_be_bytes(&self) -> Self::Bytes {
        let months = self.months().to_be_bytes();
        let days = self.days().to_be_bytes();
        let ns = self.ns().to_be_bytes();
        let mut result = [0; 16];
        result[0] = months[0];
        result[1] = months[1];
        result[2] = months[2];
        result[3] = months[3];
        result[4] = days[0];
        result[5] = days[1];
        result[6] = days[2];
        result[7] = days[3];
        (0..8).for_each(|i| {
            result[8 + i] = ns[i];
        });
        result
    }

    #[inline]
    fn from_be_bytes(bytes: Self::Bytes) -> Self {
        let mut months = [0; 4];
        months[0] = bytes[0];
        months[1] = bytes[1];
        months[2] = bytes[2];
        months[3] = bytes[3];
        let mut days = [0; 4];
        days[0] = bytes[4];
        days[1] = bytes[5];
        days[2] = bytes[6];
        days[3] = bytes[7];
        let mut ns = [0; 8];
        (0..8).for_each(|i| {
            ns[i] = bytes[8 + i];
        });
        Self(
            i32::from_be_bytes(months),
            i32::from_be_bytes(days),
            i64::from_be_bytes(ns),
        )
    }
}

create_relation!(
    months_days_ns,
    PhysicalType::Primitive(PrimitiveType::MonthDayNano)
);

impl months_days_ns {
    /// A new [`months_days_ns`].
    #[inline]
    pub fn new(months: i32, days: i32, nanoseconds: i64) -> Self {
        Self(months, days, nanoseconds)
    }

    /// The number of months
    #[inline]
    pub fn months(&self) -> i32 {
        self.0
    }

    /// The number of days
    #[inline]
    pub fn days(&self) -> i32 {
        self.1
    }

    /// The number of nanoseconds
    #[inline]
    pub fn ns(&self) -> i64 {
        self.2
    }
}

impl Neg for days_ms {
    type Output = Self;

    #[inline(always)]
    fn neg(self) -> Self::Output {
        Self([-self.0[0], -self.0[0]])
    }
}

impl Neg for months_days_ns {
    type Output = Self;

    #[inline(always)]
    fn neg(self) -> Self::Output {
        Self(-self.0, -self.1, -self.2)
    }
}
