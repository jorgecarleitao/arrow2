//! traits to handle _all native types_ used in this crate.
//! Most physical types used in this crate are native Rust types, like `i32`.
//! The most important trait is [`NativeType`], the generic trait of [`crate::array::PrimitiveArray`].
//!
//! Another important trait is [`BitChunk`], describing types that can be used to
//! represent chunks of bits (e.g. `u8`, `u16`), and [`BitChunkIter`], that can be used to
//! iterate over bitmaps in [`BitChunk`]s.
//! Finally, this module also contains traits used to compile code optimized for SIMD instructions at [`mod@simd`].
use std::cmp::Ordering;
use std::convert::TryFrom;

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
#[derive(Debug, Copy, Clone, Default, Eq, Hash)]
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

const MS_IN_DAY: i32 = 86_400_000;

impl Ord for days_ms {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_days = self.days() + self.milliseconds() / MS_IN_DAY;
        let other_days = other.days() + other.milliseconds() / MS_IN_DAY;
        let ord = self_days.cmp(&other_days);
        match ord {
            Ordering::Greater | Ordering::Less => ord,
            Ordering::Equal => {
                (self.milliseconds() % MS_IN_DAY).cmp(&(other.milliseconds() % MS_IN_DAY))
            }
        }
    }
}

impl PartialOrd for days_ms {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for days_ms {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

/// The in-memory representation of the MonthDayNano variant of the "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct months_days_ns(i32, i32, i64);

impl std::fmt::Display for months_days_ns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}m {}d {}ns", self.months(), self.days(), self.ns())
    }
}

unsafe impl NativeType for months_days_ns {
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
    #[inline]
    pub fn new(months: i32, days: i32, nanoseconds: i64) -> Self {
        Self(months, days, nanoseconds)
    }

    #[inline]
    pub fn months(&self) -> i32 {
        self.0
    }

    #[inline]
    pub fn days(&self) -> i32 {
        self.1
    }

    #[inline]
    pub fn ns(&self) -> i64 {
        self.2
    }
}

const NS_IN_DAY: i64 = 86_400_000_000_000;

impl PartialOrd for months_days_ns {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_days = self.days() as i64 + self.ns() / NS_IN_DAY;
        let other_days = other.days() as i64 + other.ns() / NS_IN_DAY;

        if self.months() == other.months() {
            // when months are equal, we have a total order
            Some((self_days, self.ns() % NS_IN_DAY).cmp(&(other_days, other.ns() % NS_IN_DAY)))
        } else {
            // Order between months=1,days=0 and months=0,days=30 is undefined because max number
            // of days in a month depends on which month we are in.
            if self_days.abs() >= 28 || other_days.abs() >= 28 {
                return None;
            }

            // when |days| is less than 28, we know it has to be smaller than 1 month, so a total
            // order can be defined.
            Some((self.months(), self_days, self.ns() % NS_IN_DAY).cmp(&(
                other.months(),
                other_days,
                other.ns() % NS_IN_DAY,
            )))
        }
    }
}

impl PartialEq for months_days_ns {
    fn eq(&self, other: &Self) -> bool {
        match self.partial_cmp(other) {
            Some(Ordering::Equal) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_ms_total_order() {
        assert_eq!(days_ms::new(1, 0), days_ms::new(1, 0));
        assert_eq!(days_ms::new(1, 0), days_ms::new(0, MS_IN_DAY));
        assert_eq!(days_ms::new(1, 0), days_ms::new(-1, MS_IN_DAY * 2));
        assert_eq!(days_ms::new(1, 0), days_ms::new(2, -MS_IN_DAY));

        assert_eq!(days_ms::new(1, 0).cmp(&days_ms::new(1, 1)), Ordering::Less);
        assert_eq!(
            days_ms::new(1, 0).cmp(&days_ms::new(2, -MS_IN_DAY + 1)),
            Ordering::Less
        );

        assert_eq!(
            days_ms::new(10, 0).cmp(&days_ms::new(1, 1)),
            Ordering::Greater
        );
        assert_eq!(
            days_ms::new(0, MS_IN_DAY + 2).cmp(&days_ms::new(1, 1)),
            Ordering::Greater
        );
        assert_eq!(
            days_ms::new(-1, MS_IN_DAY * 2 + 2).cmp(&days_ms::new(1, 1)),
            Ordering::Greater
        );
    }

    #[test]
    fn months_days_ns_partial_order() {
        assert_eq!(months_days_ns::new(1, 0, 0), months_days_ns::new(1, 0, 0));
        assert!(months_days_ns::new(1, 30, 0) != months_days_ns::new(2, 0, 0));

        // order undefined
        assert_eq!(
            months_days_ns::new(1, 28, 0).partial_cmp(&months_days_ns::new(2, 0, 0)),
            None,
        );
        assert_eq!(
            months_days_ns::new(-1, -28, 0).partial_cmp(&months_days_ns::new(-2, 0, 0)),
            None,
        );
        // ns overflow causing order to be undefined
        assert_eq!(
            months_days_ns::new(1, 27, NS_IN_DAY + 1).partial_cmp(&months_days_ns::new(2, 0, 0)),
            None,
        );
        assert_eq!(
            months_days_ns::new(1, -27, -NS_IN_DAY - 1).partial_cmp(&months_days_ns::new(2, 0, 0)),
            None,
        );

        // total order when months are not equal, but |days| < 28
        assert_eq!(
            months_days_ns::new(1, 27, 0).partial_cmp(&months_days_ns::new(2, 0, 0)),
            Some(Ordering::Less),
        );
        assert_eq!(
            months_days_ns::new(1, 27, 0).partial_cmp(&months_days_ns::new(-2, 12, 1)),
            Some(Ordering::Greater),
        );

        // total order when months are equal
        assert_eq!(
            months_days_ns::new(2, -28, 0).partial_cmp(&months_days_ns::new(2, 0, 0)),
            Some(Ordering::Less),
        );
        assert_eq!(
            months_days_ns::new(0, 28, 0).partial_cmp(&months_days_ns::new(0, 0, 1)),
            Some(Ordering::Greater),
        );
    }
}
