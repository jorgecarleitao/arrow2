//! traits to handle _all physical types_ used in this crate.
//! Most physical types used in this crate are native Rust types, like `i32`.
//! The most important trait is [`NativeType`], implemented for all Arrow types
//! with a Rust correspondence (such as `i32` or `f64`).
//!
//! Another important trait is [`BitChunk`], describing types that can be used to
//! represent chunks of bits (e.g. `u8`, `u16`), and [`BitChunkIter`], that can be used to
//! iterate over bitmaps in [`BitChunk`]s.
//! Finally, this module also contains traits used to compile code optimized for SIMD instructions at [`mod@simd`].
use std::convert::TryFrom;

mod bit_chunk;
pub use bit_chunk::{BitChunk, BitChunkIter};
mod index;
pub mod simd;
pub use index::*;

use crate::datatypes::{DataType, IntervalUnit, PhysicalType};

/// Trait denoting anything that has a natural logical [`DataType`].
/// For example, [`DataType::Int32`] for `i32`.
pub trait NaturalDataType {
    /// The natural [`DataType`].
    const DATA_TYPE: DataType;
}

/// describes whether a [`DataType`] is valid.
pub unsafe trait Relation {
    /// Whether `data_type` is a valid [`DataType`].
    fn is_valid(data_type: &DataType) -> bool;
}

macro_rules! create_relation {
    ($native_ty:ty, $physical_ty:expr) => {
        unsafe impl Relation for $native_ty {
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

/// Declares any type that can be allocated, serialized and deserialized by this crate.
/// All data-heavy memory operations are implemented for this trait alone.
/// # Safety
/// Do not implement.
pub unsafe trait NativeType:
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
    + Sized
    + 'static
{
    /// Type denoting its representation as bytes
    type Bytes: AsRef<[u8]> + for<'a> TryFrom<&'a [u8]>;

    /// To bytes in little endian
    fn to_le_bytes(&self) -> Self::Bytes;

    /// To bytes in big endian
    fn to_be_bytes(&self) -> Self::Bytes;

    /// From bytes in big endian
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
natural_type!(i128, DataType::Decimal(32, 32)); // users should set the decimal when creating an array

create_relation!(u8, PhysicalType::UInt8);
create_relation!(u16, PhysicalType::UInt16);
create_relation!(u32, PhysicalType::UInt32);
create_relation!(u64, PhysicalType::UInt64);
create_relation!(i8, PhysicalType::Int8);
create_relation!(i16, PhysicalType::Int16);
create_relation!(i32, PhysicalType::Int32);
create_relation!(i64, PhysicalType::Int64);
create_relation!(i128, PhysicalType::Int128);
create_relation!(f32, PhysicalType::Float32);
create_relation!(f64, PhysicalType::Float64);

/// The in-memory representation of the DayMillisecond variant of arrow's "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
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

create_relation!(days_ms, PhysicalType::DaysMs);

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
