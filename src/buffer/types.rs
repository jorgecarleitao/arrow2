use crate::datatypes::{DataType, IntervalUnit};

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

#[derive(Debug, Copy, Clone, Default, PartialEq)]
#[allow(non_camel_case_types)]
pub struct days_ms([i32; 2]);

impl std::fmt::Display for days_ms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}d {}ms", self.0[0], self.0[1])
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
