use std::unimplemented;

use crate::{
    datatypes::{DataType, IntervalUnit},
    types::{days_ms, NativeType},
};

use super::{
    primitive::PrimitiveArray, Array, BinaryArray, BooleanArray, DictionaryArray, DictionaryKey,
    FixedSizeBinaryArray, FixedSizeListArray, ListArray, NullArray, Offset, StructArray, Utf8Array,
};

mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod struct_;
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
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<i8>(lhs, rhs)
            }
            DataType::Int16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<i16>(lhs, rhs)
            }
            DataType::Int32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<i32>(lhs, rhs)
            }
            DataType::Int64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<i64>(lhs, rhs)
            }
            DataType::UInt8 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<u8>(lhs, rhs)
            }
            DataType::UInt16 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<u16>(lhs, rhs)
            }
            DataType::UInt32 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<u32>(lhs, rhs)
            }
            DataType::UInt64 => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                dictionary::equal::<u64>(lhs, rhs)
            }
            _ => unreachable!(),
        },
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
        DataType::Union(_) => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{BooleanArray, Int16Array, Int32Array, Offset};

    use super::*;

    #[test]
    fn test_primitive() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(3)],
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(4)],
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                vec![Some(1), Some(2), None],
                true,
            ),
            (
                vec![Some(1), None, Some(3)],
                vec![Some(1), Some(2), None],
                false,
            ),
            (
                vec![Some(1), None, None],
                vec![Some(1), Some(2), None],
                false,
            ),
        ];

        for (lhs, rhs, expected) in cases {
            let lhs = Int32Array::from(&lhs);
            let rhs = Int32Array::from(&rhs);
            test_equal(&lhs, &rhs, expected);
        }
    }

    #[test]
    fn test_primitive_slice() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                (1, 1),
                vec![Some(1), Some(2), Some(3)],
                (2, 1),
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                (1, 1),
                vec![Some(1), None, Some(2)],
                (2, 1),
                true,
            ),
            (
                vec![None, Some(2), None],
                (1, 1),
                vec![None, None, Some(2)],
                (2, 1),
                true,
            ),
            (
                vec![Some(1), None, Some(2), None, Some(3)],
                (2, 2),
                vec![None, Some(2), None, Some(3)],
                (1, 2),
                true,
            ),
        ];

        for (lhs, slice_lhs, rhs, slice_rhs, expected) in cases {
            let lhs = Int32Array::from(&lhs);
            let lhs = lhs.slice(slice_lhs.0, slice_lhs.1);
            let rhs = Int32Array::from(&rhs);
            let rhs = rhs.slice(slice_rhs.0, slice_rhs.1);

            test_equal(&lhs, &rhs, expected);
        }
    }

    pub(super) fn test_equal(lhs: &dyn Array, rhs: &dyn Array, expected: bool) {
        // equality is symmetric
        assert_eq!(equal(lhs, lhs), true, "\n{:?}\n{:?}", lhs, lhs);
        assert_eq!(equal(rhs, rhs), true, "\n{:?}\n{:?}", rhs, rhs);

        assert_eq!(equal(lhs, rhs), expected, "\n{:?}\n{:?}", lhs, rhs);
        assert_eq!(equal(rhs, lhs), expected, "\n{:?}\n{:?}", rhs, lhs);
    }

    #[test]
    fn test_boolean_equal() {
        let a = BooleanArray::from_slice([false, false, true]);
        let b = BooleanArray::from_slice([false, false, true]);
        test_equal(&a, &b, true);

        let b = BooleanArray::from_slice([false, false, false]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_boolean_equal_null() {
        let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        test_equal(&a, &b, true);

        let b = BooleanArray::from(vec![None, None, None, Some(true)]);
        test_equal(&a, &b, false);

        let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_boolean_equal_offset() {
        let a = BooleanArray::from_slice(vec![false, true, false, true, false, false, true]);
        let b = BooleanArray::from_slice(vec![true, false, false, false, true, false, true, true]);
        test_equal(&a, &b, false);

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(3, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(3, 4);
        let b_slice = b.slice(4, 4);
        test_equal(&a_slice, &b_slice, false);

        // Elements fill in `u8`'s exactly.
        let mut vector = vec![false, false, true, true, true, true, true, true];
        let a = BooleanArray::from_slice(vector.clone());
        let b = BooleanArray::from_slice(vector.clone());
        test_equal(&a, &b, true);

        // Elements fill in `u8`s + suffix bits.
        vector.push(true);
        let a = BooleanArray::from_slice(vector.clone());
        let b = BooleanArray::from_slice(vector);
        test_equal(&a, &b, true);
    }

    #[allow(clippy::type_complexity)]
    fn binary_cases() -> Vec<(Vec<Option<String>>, Vec<Option<String>>, bool)> {
        let base = vec![
            Some("hello".to_owned()),
            None,
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        let not_base = vec![
            Some("hello".to_owned()),
            Some("foo".to_owned()),
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        vec![
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                true,
            ),
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("arrow".to_owned())],
                false,
            ),
            (base.clone(), base.clone(), true),
            (base, not_base, false),
        ]
    }

    fn test_generic_string_equal<O: Offset>() {
        let cases = binary_cases();

        for (lhs, rhs, expected) in cases {
            let lhs = lhs.iter().map(|x| x.as_deref()).collect::<Vec<_>>();
            let rhs = rhs.iter().map(|x| x.as_deref()).collect::<Vec<_>>();
            let lhs = Utf8Array::<O>::from(&lhs);
            let rhs = Utf8Array::<O>::from(&rhs);
            test_equal(&lhs, &rhs, expected);
        }
    }

    #[test]
    fn test_string_equal() {
        test_generic_string_equal::<i32>()
    }

    #[test]
    fn test_large_string_equal() {
        test_generic_string_equal::<i64>()
    }

    fn create_dictionary_array(values: &[&str], keys: &[Option<i16>]) -> DictionaryArray<i16> {
        let keys = Int16Array::from(keys);
        let values = Utf8Array::<i32>::from_slice(values);

        DictionaryArray::from_data(keys, Arc::new(values))
    }

    #[test]
    fn test_dictionary_equal() {
        // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
        let a = create_dictionary_array(&["a", "b", "c"], &[Some(0), Some(1), Some(0), Some(2)]);
        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), Some(2), Some(0), Some(1)]);
        test_equal(&a, &b, true);

        // different len
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), Some(2), Some(1)]);
        test_equal(&a, &b, false);

        // different key
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), Some(2), Some(0), Some(0)]);
        test_equal(&a, &b, false);

        // different values, same keys
        let b = create_dictionary_array(&["a", "b", "d"], &[Some(0), Some(1), Some(0), Some(2)]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_dictionary_equal_null() {
        // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
        let a = create_dictionary_array(&["a", "b", "c"], &[Some(0), None, Some(0), Some(2)]);

        // equal to self
        test_equal(&a, &a, true);

        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), None, Some(0), Some(1)]);
        test_equal(&a, &b, true);

        // different null position
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), Some(2), Some(0), None]);
        test_equal(&a, &b, false);

        // different key
        let b = create_dictionary_array(&["a", "c", "b"], &[Some(0), None, Some(0), Some(0)]);
        test_equal(&a, &b, false);

        // different values, same keys
        let b = create_dictionary_array(&["a", "b", "d"], &[Some(0), None, Some(0), Some(2)]);
        test_equal(&a, &b, false);
    }
}
