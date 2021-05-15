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
        DataType::UInt64,
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
        hash_primitive::<$ty>($array.as_any().downcast_ref().unwrap())
    }};
}

/// Returns the element-wise hash of an [`Array`]. Validity is preserved.
/// Supported DataTypes:
/// * Boolean types
/// * All primitive types except `Float32` and `Float64`
/// * `[Large]Utf8`;
/// * `[Large]Binary`.
/// # Errors
/// This function errors whenever it does not support the specific `DataType`.
pub fn hash(array: &dyn Array) -> Result<PrimitiveArray<u64>> {
    Ok(match array.data_type() {
        DataType::Boolean => hash_boolean(array.as_any().downcast_ref().unwrap()),
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
        DataType::Binary => hash_binary::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeBinary => hash_binary::<i64>(array.as_any().downcast_ref().unwrap()),
        DataType::Utf8 => hash_utf8::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeUtf8 => hash_utf8::<i64>(array.as_any().downcast_ref().unwrap()),
        t => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Hash not implemented for type {:?}",
                t
            )))
        }
    })
}

/// Checks if an array of type `datatype` can perform hash operation
///
/// # Examples
/// ```
/// use arrow2::compute::hash::can_hash;
/// use arrow2::datatypes::{DataType};
///
/// let data_type = DataType::Int8;
/// assert_eq!(can_hash(&data_type), true);

/// let data_type = DataType::Null;
/// assert_eq!(can_hash(&data_type), false);
/// ```
pub fn can_hash(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(_)
        | DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal(_, _)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8 => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency() {
        use crate::array::new_null_array;
        use crate::datatypes::DataType::*;
        use crate::datatypes::TimeUnit;

        let datatypes = vec![
            Null,
            Boolean,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Int8,
            Int16,
            Int32,
            Int64,
            Float32,
            Float64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Date32,
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Date64,
            Utf8,
            LargeUtf8,
            Binary,
            LargeBinary,
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
        ];

        datatypes.clone().into_iter().for_each(|d1| {
            let array = new_null_array(d1.clone(), 10);
            if can_hash(&d1) {
                assert!(hash(array.as_ref()).is_ok());
            } else {
                assert!(hash(array.as_ref()).is_err());
            }
        });
    }
}
