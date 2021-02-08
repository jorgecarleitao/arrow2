use crate::datatypes::DataType;

pub unsafe trait NativeType:
    Sized + Copy + std::fmt::Debug + std::fmt::Display + PartialEq + Default + Sized + 'static
{
    fn is_valid(data_type: &DataType) -> bool;
}

unsafe impl NativeType for u8 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt8
    }
}

unsafe impl NativeType for u16 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt16
    }
}

unsafe impl NativeType for u32 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt32
    }
}
unsafe impl NativeType for u64 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt64
    }
}

unsafe impl NativeType for i8 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int8
    }
}

unsafe impl NativeType for i16 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int16
    }
}

unsafe impl NativeType for i32 {
    fn is_valid(data_type: &DataType) -> bool {
        match data_type {
            DataType::Int32 | DataType::Date32 | DataType::Time32(_) => true,
            _ => false,
        }
    }
}

unsafe impl NativeType for i64 {
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
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float32
    }
}

unsafe impl NativeType for f64 {
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float64
    }
}

unsafe impl NativeType for i128 {
    fn is_valid(data_type: &DataType) -> bool {
        if let DataType::Decimal(_, _) = data_type {
            true
        } else {
            false
        }
    }
}
