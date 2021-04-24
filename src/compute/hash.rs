use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::{
    array::{Array, BinaryArray, BooleanArray, Offset, PrimitiveArray, Utf8Array},
    buffer::Buffer,
    datatypes::{DataType, IntervalUnit},
    error::{ArrowError, Result},
    types::{days_ms, NativeType},
};

use super::arity::unary;

/// Element-wise hash of a [`PrimitiveArray`]. Validity is preserved.
pub fn hash_primitive<T: NativeType + Hash>(array: &PrimitiveArray<T>) -> PrimitiveArray<u64> {
    unary(
        array,
        |x| {
            let mut hasher = DefaultHasher::new();
            x.hash(&mut hasher);
            hasher.finish()
        },
        &DataType::UInt64,
    )
}

/// Element-wise hash of a [`BooleanArray`]. Validity is preserved.
pub fn hash_boolean(array: &BooleanArray) -> PrimitiveArray<u64> {
    let iter = array.values_iter().map(|x| {
        let mut hasher = DefaultHasher::new();
        x.hash(&mut hasher);
        hasher.finish()
    });
    let values = Buffer::from_trusted_len_iter(iter);
    PrimitiveArray::<u64>::from_data(DataType::UInt64, values, array.validity().clone())
}

/// Element-wise hash of a [`Utf8Array`]. Validity is preserved.
pub fn hash_utf8<O: Offset>(array: &Utf8Array<O>) -> PrimitiveArray<u64> {
    let iter = array.values_iter().map(|x| {
        let mut hasher = DefaultHasher::new();
        x.hash(&mut hasher);
        hasher.finish()
    });
    let values = Buffer::from_trusted_len_iter(iter);
    PrimitiveArray::<u64>::from_data(DataType::UInt64, values, array.validity().clone())
}

/// Element-wise hash of a [`BinaryArray`]. Validity is preserved.
pub fn hash_binary<O: Offset>(array: &BinaryArray<O>) -> PrimitiveArray<u64> {
    let iter = array.values_iter().map(|x| {
        let mut hasher = DefaultHasher::new();
        x.hash(&mut hasher);
        hasher.finish()
    });
    let values = Buffer::from_trusted_len_iter(iter);
    PrimitiveArray::<u64>::from_data(DataType::UInt64, values, array.validity().clone())
}

macro_rules! hash_dyn {
    ($ty:ty, $array:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        hash_primitive(array)
    }};
}

/// Returns the element-wise hash of an [`Array`]. Validity is preserved.
/// Supported DataTypes:
/// * All primitive types except `Float32` and `Float64`
/// * `[Large]Utf8`;
/// * `[Large]Binary`.
/// # Errors
/// This function errors whenever it does not support the specific `DataType`.
pub fn hash(array: &dyn Array) -> Result<PrimitiveArray<u64>> {
    Ok(match array.data_type() {
        DataType::Int8 => hash_dyn!(i8, array),
        DataType::Int16 => hash_dyn!(i16, array),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => hash_dyn!(i32, array),
        DataType::Interval(IntervalUnit::DayTime) => hash_dyn!(days_ms, array),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => hash_dyn!(i64, array),
        DataType::Decimal(_, _) => hash_dyn!(i128, array),
        DataType::UInt8 => hash_dyn!(u8, array),
        DataType::UInt16 => hash_dyn!(u16, array),
        DataType::UInt32 => hash_dyn!(u32, array),
        DataType::UInt64 => hash_dyn!(u64, array),
        DataType::Float16 => unreachable!(),
        DataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            hash_binary(array)
        }
        DataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            hash_binary(array)
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            hash_utf8(array)
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            hash_utf8(array)
        }
        t => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Hash not implemented for type {:?}",
                t
            )))
        }
    })
}
