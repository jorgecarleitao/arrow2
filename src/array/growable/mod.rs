// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains components suitable to create an array out of N other arrays, by slicing
//! from each in a random-access fashion.

use crate::datatypes::*;

use super::Array;

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

mod utils;

pub trait Growable<'a> {
    /// Extends this [`GrowableArray`] with elements from the bounded [`Array`] at `start`
    /// and for a size of `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`GrowableArray`] with null elements, disregarding the bound arrays
    fn extend_nulls(&mut self, additional: usize);

    /// Converts itself to an `Arc<dyn Array>`, thereby finishing the mutation.
    /// Self will be empty after such operation
    fn to_arc(&mut self) -> std::sync::Arc<dyn Array>;
}

/// # Panics
/// This function panics iff the arrays do not have the same data_type.
pub fn make_growable<'a>(
    arrays: &[&'a dyn Array],
    use_nulls: bool,
    capacity: usize,
) -> Box<dyn Growable<'a> + 'a> {
    assert!(!arrays.is_empty());
    let data_type = arrays[0].data_type();
    assert!(arrays.iter().all(|&item| item.data_type() == data_type));

    match data_type {
        DataType::Null => Box::new(null::GrowableNull::new()),
        DataType::Boolean => Box::new(boolean::GrowableBoolean::new(arrays, use_nulls, capacity)),
        DataType::Int8 => Box::new(primitive::GrowablePrimitive::<i8>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::Int16 => Box::new(primitive::GrowablePrimitive::<i16>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(primitive::GrowablePrimitive::<i32>::new(
                arrays, use_nulls, capacity,
            ))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => Box::new(
            primitive::GrowablePrimitive::<i64>::new(arrays, use_nulls, capacity),
        ),
        DataType::Decimal(_, _) => Box::new(primitive::GrowablePrimitive::<i128>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::UInt8 => Box::new(primitive::GrowablePrimitive::<u8>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::UInt16 => Box::new(primitive::GrowablePrimitive::<u16>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::UInt32 => Box::new(primitive::GrowablePrimitive::<u32>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::UInt64 => Box::new(primitive::GrowablePrimitive::<u64>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(primitive::GrowablePrimitive::<f32>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::Float64 => Box::new(primitive::GrowablePrimitive::<f64>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::Utf8 => Box::new(utf8::GrowableUtf8::<i32>::new(arrays, use_nulls, capacity)),
        DataType::LargeUtf8 => {
            Box::new(utf8::GrowableUtf8::<i64>::new(arrays, use_nulls, capacity))
        }
        DataType::Binary => Box::new(binary::GrowableBinary::<i32>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::LargeBinary => Box::new(binary::GrowableBinary::<i64>::new(
            arrays, use_nulls, capacity,
        )),
        DataType::FixedSizeBinary(_) => Box::new(fixed_binary::GrowableFixedSizeBinary::new(
            arrays, use_nulls, capacity,
        )),

        DataType::List(_) => Box::new(list::GrowableList::<i32>::new(arrays, use_nulls, capacity)),
        DataType::LargeList(_) => {
            Box::new(list::GrowableList::<i64>::new(arrays, use_nulls, capacity))
        }
        DataType::Struct(_) => {
            Box::new(structure::GrowableStruct::new(arrays, use_nulls, capacity))
        }
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::Union(_) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
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

    #[test]
    fn test_dictionary() {
        // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
        let array = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), Some("b"), None, Some("c")],
        );
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = DictionaryArray::from(Arc::new(result));

        let expected = Int16Array::from(vec![Some(1), None]);
        assert_eq!(result.keys(), &expected);
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
