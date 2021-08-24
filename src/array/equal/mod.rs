use crate::{
    datatypes::{DataType, IntervalUnit},
    types::{days_ms, NativeType},
};

use super::*;

mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod struct_;
mod union;
mod utf8;

impl PartialEq for dyn Array {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<NullArray> for NullArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for NullArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<T: NativeType> PartialEq<&dyn Array> for PrimitiveArray<T> {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<T: NativeType> PartialEq<PrimitiveArray<T>> for PrimitiveArray<T> {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<BooleanArray> for BooleanArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for BooleanArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<O: Offset> PartialEq<Utf8Array<O>> for Utf8Array<O> {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl<O: Offset> PartialEq<&dyn Array> for Utf8Array<O> {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<O: Offset> PartialEq<BinaryArray<O>> for BinaryArray<O> {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl<O: Offset> PartialEq<&dyn Array> for BinaryArray<O> {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl PartialEq<FixedSizeBinaryArray> for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for FixedSizeBinaryArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<O: Offset> PartialEq<ListArray<O>> for ListArray<O> {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl<O: Offset> PartialEq<&dyn Array> for ListArray<O> {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl PartialEq<FixedSizeListArray> for FixedSizeListArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for FixedSizeListArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl PartialEq<StructArray> for StructArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for StructArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl<K: DictionaryKey> PartialEq<DictionaryArray<K>> for DictionaryArray<K> {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl<K: DictionaryKey> PartialEq<&dyn Array> for DictionaryArray<K> {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

impl PartialEq<UnionArray> for UnionArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

impl PartialEq<&dyn Array> for UnionArray {
    fn eq(&self, other: &&dyn Array) -> bool {
        equal(self, *other)
    }
}

/// Logically compares two [`Array`]s.
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * each of their items are equal
pub fn equal(lhs: &dyn Array, rhs: &dyn Array) -> bool {
    if lhs.data_type() != rhs.data_type() {
        return false;
    }

    match lhs.data_type() {
        DataType::Null => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            null::equal(lhs, rhs)
        }
        DataType::Boolean => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            boolean::equal(lhs, rhs)
        }
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<u8>(lhs, rhs)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<u16>(lhs, rhs)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<u32>(lhs, rhs)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<u64>(lhs, rhs)
        }
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<i8>(lhs, rhs)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<i16>(lhs, rhs)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<i32>(lhs, rhs)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<i64>(lhs, rhs)
        }
        DataType::Decimal(_, _) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<i128>(lhs, rhs)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<days_ms>(lhs, rhs)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<f32>(lhs, rhs)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            primitive::equal::<f64>(lhs, rhs)
        }
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::equal::<i32>(lhs, rhs)
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            utf8::equal::<i64>(lhs, rhs)
        }
        DataType::Binary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::equal::<i32>(lhs, rhs)
        }
        DataType::LargeBinary => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            binary::equal::<i64>(lhs, rhs)
        }
        DataType::List(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            list::equal::<i32>(lhs, rhs)
        }
        DataType::LargeList(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            list::equal::<i64>(lhs, rhs)
        }
        DataType::Struct(_) => {
            let lhs = lhs.as_any().downcast_ref::<StructArray>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<StructArray>().unwrap();
            struct_::equal(lhs, rhs)
        }
        DataType::Dictionary(key_type, _) => {
            with_match_dictionary_key_type!(key_type.as_ref(), |$T| {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<$T>(lhs, rhs)
            })
        }
        DataType::FixedSizeBinary(_) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            fixed_size_binary::equal(lhs, rhs)
        }
        DataType::FixedSizeList(_, _) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            fixed_size_list::equal(lhs, rhs)
        }
        DataType::Union(_, _, _) => {
            let lhs = lhs.as_any().downcast_ref().unwrap();
            let rhs = rhs.as_any().downcast_ref().unwrap();
            union::equal(lhs, rhs)
        }
    }
}
