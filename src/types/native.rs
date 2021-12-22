use std::convert::TryFrom;
use std::ops::Neg;

use super::PrimitiveType;

/// Sealed trait implemented by all physical types that can be allocated,
/// serialized and deserialized by this crate.
/// All O(N) allocations in this crate are done for this trait alone.
pub trait NativeType:
    super::private::Sealed
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
    /// The corresponding variant of [`PrimitiveType`].
    const PRIMITIVE: PrimitiveType;

    /// Type denoting its representation as bytes.
    /// This is `[u8; N]` where `N = size_of::<T>`.
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

macro_rules! native_type {
    ($type:ty, $primitive_type:expr) => {
        impl NativeType for $type {
            const PRIMITIVE: PrimitiveType = $primitive_type;

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

native_type!(u8, PrimitiveType::UInt8);
native_type!(u16, PrimitiveType::UInt16);
native_type!(u32, PrimitiveType::UInt32);
native_type!(u64, PrimitiveType::UInt64);
native_type!(i8, PrimitiveType::Int8);
native_type!(i16, PrimitiveType::Int16);
native_type!(i32, PrimitiveType::Int32);
native_type!(i64, PrimitiveType::Int64);
native_type!(f32, PrimitiveType::Float32);
native_type!(f64, PrimitiveType::Float64);
native_type!(i128, PrimitiveType::Int128);

/// The in-memory representation of the DayMillisecond variant of arrow's "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub struct days_ms([i32; 2]);

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

impl NativeType for days_ms {
    const PRIMITIVE: PrimitiveType = PrimitiveType::DaysMs;
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

/// The in-memory representation of the MonthDayNano variant of the "Interval" logical type.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct months_days_ns(i32, i32, i64);

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

impl NativeType for months_days_ns {
    const PRIMITIVE: PrimitiveType = PrimitiveType::MonthDayNano;
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

impl std::fmt::Display for days_ms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}d {}ms", self.days(), self.milliseconds())
    }
}

impl std::fmt::Display for months_days_ns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}m {}d {}ns", self.months(), self.days(), self.ns())
    }
}

impl Neg for days_ms {
    type Output = Self;

    #[inline(always)]
    fn neg(self) -> Self::Output {
        Self::new(-self.days(), -self.milliseconds())
    }
}

impl Neg for months_days_ns {
    type Output = Self;

    #[inline(always)]
    fn neg(self) -> Self::Output {
        Self::new(-self.months(), -self.days(), -self.ns())
    }
}
