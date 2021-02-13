use crate::datatypes::DataType;

pub unsafe trait NativeType:
    Sized + Copy + std::fmt::Debug + std::fmt::Display + PartialEq + Default + Sized + 'static
{
    type Bytes: AsRef<[u8]>;

    fn is_valid(data_type: &DataType) -> bool;

    // Vec is not ideal, but well...
    fn to_le_bytes(&self) -> Vec<u8>;
}

unsafe impl NativeType for u8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt8
    }
}

unsafe impl NativeType for u16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt16
    }
}

unsafe impl NativeType for u32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt32
    }
}

unsafe impl NativeType for u64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt64
    }
}

unsafe impl NativeType for i8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int8
    }
}

unsafe impl NativeType for i16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int16
    }
}

unsafe impl NativeType for i32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int32 | DataType::Date32 | DataType::Time32(_) => true,
            _ => false,
        }
    }
}

unsafe impl NativeType for i64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _) => true,
            _ => false,
        }
    }
}

unsafe impl NativeType for f32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float32
    }
}

unsafe impl NativeType for f64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float64
    }
}

unsafe impl NativeType for i128 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Vec<u8> {
        Self::to_le_bytes(*self).to_vec()
    }

    fn is_valid(data_type: &DataType) -> bool {
        if let DataType::Decimal(_, _) = data_type {
            true
        } else {
            false
        }
    }
}
