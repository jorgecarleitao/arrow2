//! Contains components suitable to create an array out of N other arrays, by slicing
//! from each in a random-access fashion.

use crate::array::*;
use crate::datatypes::*;

mod binary;
pub use binary::GrowableBinary;
mod boolean;
pub use boolean::GrowableBoolean;
mod fixed_binary;
pub use fixed_binary::GrowableFixedSizeBinary;
mod null;
pub use null::GrowableNull;
mod primitive;
pub use primitive::GrowablePrimitive;
mod list;
pub use list::GrowableList;
mod structure;
pub use structure::GrowableStruct;
mod utf8;
pub use utf8::GrowableUtf8;
mod dictionary;
pub use dictionary::GrowableDictionary;

mod utils;

pub trait Growable<'a> {
    /// Extends this [`GrowableArray`] with elements from the bounded [`Array`] at `start`
    /// and for a size of `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`GrowableArray`] with null elements, disregarding the bound arrays
    fn extend_validity(&mut self, additional: usize);

    /// Converts itself to an `Arc<dyn Array>`, thereby finishing the mutation.
    /// Self will be empty after such operation
    fn to_arc(&mut self) -> std::sync::Arc<dyn Array> {
        self.to_box().into()
    }

    /// Converts itself to an `Box<dyn Array>`, thereby finishing the mutation.
    /// Self will be empty after such operation
    fn to_box(&mut self) -> Box<dyn Array>;
}

macro_rules! dyn_growable {
    ($ty:ty, $arrays:expr, $use_validity:expr, $capacity:expr) => {{
        let arrays = $arrays
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$ty>>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Box::new(primitive::GrowablePrimitive::<$ty>::new(
            &arrays,
            $use_validity,
            $capacity,
        ))
    }};
}

macro_rules! dyn_dict_growable {
    ($ty:ty, $arrays:expr, $use_validity:expr, $capacity:expr) => {{
        let arrays = $arrays
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$ty>>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Box::new(dictionary::GrowableDictionary::<$ty>::new(
            &arrays,
            $use_validity,
            $capacity,
        ))
    }};
}

/// # Panics
/// This function panics iff the arrays do not have the same data_type.
pub fn make_growable<'a>(
    arrays: &[&'a dyn Array],
    use_validity: bool,
    capacity: usize,
) -> Box<dyn Growable<'a> + 'a> {
    assert!(!arrays.is_empty());
    let data_type = arrays[0].data_type();
    assert!(arrays.iter().all(|&item| item.data_type() == data_type));

    match data_type {
        DataType::Null => Box::new(null::GrowableNull::new()),
        DataType::Boolean => Box::new(boolean::GrowableBoolean::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::Int8 => dyn_growable!(i8, arrays, use_validity, capacity),
        DataType::Int16 => dyn_growable!(i16, arrays, use_validity, capacity),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_growable!(i32, arrays, use_validity, capacity)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            dyn_growable!(i64, arrays, use_validity, capacity)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_growable!(days_ms, arrays, use_validity, capacity)
        }
        DataType::Decimal(_, _) => dyn_growable!(i128, arrays, use_validity, capacity),
        DataType::UInt8 => dyn_growable!(u8, arrays, use_validity, capacity),
        DataType::UInt16 => dyn_growable!(u16, arrays, use_validity, capacity),
        DataType::UInt32 => dyn_growable!(u32, arrays, use_validity, capacity),
        DataType::UInt64 => dyn_growable!(u64, arrays, use_validity, capacity),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => dyn_growable!(f32, arrays, use_validity, capacity),
        DataType::Float64 => dyn_growable!(f64, arrays, use_validity, capacity),
        DataType::Utf8 => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap())
                .collect::<Vec<_>>();
            Box::new(utf8::GrowableUtf8::<i32>::new(
                &arrays,
                use_validity,
                capacity,
            ))
        }
        DataType::LargeUtf8 => {
            let arrays = arrays
                .iter()
                .map(|array| array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap())
                .collect::<Vec<_>>();
            Box::new(utf8::GrowableUtf8::<i64>::new(
                &arrays,
                use_validity,
                capacity,
            ))
        }
        DataType::Binary => Box::new(binary::GrowableBinary::<i32>::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::LargeBinary => Box::new(binary::GrowableBinary::<i64>::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::FixedSizeBinary(_) => Box::new(fixed_binary::GrowableFixedSizeBinary::new(
            arrays,
            use_validity,
            capacity,
        )),

        DataType::List(_) => Box::new(list::GrowableList::<i32>::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::LargeList(_) => Box::new(list::GrowableList::<i64>::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::Struct(_) => Box::new(structure::GrowableStruct::new(
            arrays,
            use_validity,
            capacity,
        )),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::Union(_) => todo!(),
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::UInt8 => dyn_dict_growable!(u8, arrays, use_validity, capacity),
            DataType::UInt16 => dyn_dict_growable!(u16, arrays, use_validity, capacity),
            DataType::UInt32 => dyn_dict_growable!(u32, arrays, use_validity, capacity),
            DataType::UInt64 => dyn_dict_growable!(u64, arrays, use_validity, capacity),
            DataType::Int8 => dyn_dict_growable!(i8, arrays, use_validity, capacity),
            DataType::Int16 => dyn_dict_growable!(i16, arrays, use_validity, capacity),
            DataType::Int32 => dyn_dict_growable!(i32, arrays, use_validity, capacity),
            DataType::Int64 => dyn_dict_growable!(i64, arrays, use_validity, capacity),
            _ => unreachable!(),
        },
    }
}

/*
#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;

    use crate::{
        array::{
            Array, ArrayDataRef, ArrayRef, BooleanArray, DictionaryArray,
            FixedSizeBinaryArray, Int16Array, Int16Type, Int32Array, Int64Array,
            Int64Builder, ListBuilder, NullArray, PrimitiveBuilder, StringArray,
            StringDictionaryBuilder, StructArray, UInt8Array,
        },
        buffer::Buffer,
        datatypes::Field,
    };
    use crate::{
        array::{ListArray, StringBuilder},
        error::Result,
    };

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayDataRef {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::new_with_dictionary(
            PrimitiveBuilder::<Int16Type>::new(3),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null().unwrap()
            }
        }
        builder.finish().data()
    }

    /*
    // this is an old test used on a meanwhile removed dead code
    // that is still useful when `MutableArrayData` supports fixed-size lists.
    #[test]
    fn test_fixed_size_list_append() -> Result<()> {
        let int_builder = UInt16Builder::new(64);
        let mut builder = FixedSizeListBuilder::<UInt16Builder>::new(int_builder, 2);
        builder.values().append_slice(&[1, 2])?;
        builder.append(true)?;
        builder.values().append_slice(&[3, 4])?;
        builder.append(false)?;
        builder.values().append_slice(&[5, 6])?;
        builder.append(true)?;

        let a_builder = UInt16Builder::new(64);
        let mut a_builder = FixedSizeListBuilder::<UInt16Builder>::new(a_builder, 2);
        a_builder.values().append_slice(&[7, 8])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[9, 10])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[11, 12])?;
        a_builder.append(false)?;
        a_builder.values().append_slice(&[13, 14])?;
        a_builder.append(true)?;
        a_builder.values().append_null()?;
        a_builder.values().append_null()?;
        a_builder.append(true)?;
        let a = a_builder.finish();

        // append array
        builder.append_data(&[
            a.data(),
            a.slice(1, 3).data(),
            a.slice(2, 1).data(),
            a.slice(5, 0).data(),
        ])?;
        let finished = builder.finish();

        let expected_int_array = UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            // append first array
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            None,
            None,
            // append slice(1, 3)
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            // append slice(2, 1)
            Some(11),
            Some(12),
        ]);
        let expected_list_data = ArrayData::new(
            DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt16, true)),
                2,
            ),
            12,
            None,
            None,
            0,
            vec![],
            vec![expected_int_array.data()],
        );
        let expected_list =
            FixedSizeListArray::from(Arc::new(expected_list_data) as ArrayDataRef);
        assert_eq!(&expected_list.values(), &finished.values());
        assert_eq!(expected_list.len(), finished.len());

        Ok(())
    }
    */
}
*/
