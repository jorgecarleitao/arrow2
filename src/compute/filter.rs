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

use crate::record_batch::RecordBatch;
use crate::{
    array::growable::make_growable,
    datatypes::{DataType, IntervalUnit},
};
use crate::{array::growable::Growable, bits::SlicesIterator};
use crate::{array::*, bitmap::Bitmap, types::NativeType};
use crate::{bitmap::MutableBitmap, types::days_ms};
use crate::{buffer::MutableBuffer, error::Result};

/// Function that can filter arbitrary arrays
pub type Filter<'a> = Box<dyn Fn(&dyn Array) -> Box<dyn Array> + 'a>;

fn filter_nonnull_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    mask: &Bitmap,
) -> PrimitiveArray<T> {
    assert_eq!(array.len(), mask.len());
    let filter_count = mask.len() - mask.null_count();

    if let Some(validity) = array.validity() {
        let mut buffer = MutableBuffer::<T>::with_capacity(filter_count);
        let mut new_validity = MutableBitmap::with_capacity(filter_count);

        array
            .values()
            .iter()
            .zip(validity.iter())
            .zip(mask.iter())
            .filter(|x| x.1)
            .map(|x| x.0)
            .for_each(|(item, is_valid)| unsafe {
                buffer.push_unchecked(*item);
                new_validity.push_unchecked(is_valid);
            });

        PrimitiveArray::<T>::from_data(
            array.data_type().clone(),
            buffer.into(),
            new_validity.into(),
        )
    } else {
        let mut buffer = MutableBuffer::<T>::with_capacity(filter_count);

        array
            .values()
            .iter()
            .zip(mask.iter())
            .filter(|x| x.1)
            .map(|x| x.0)
            .for_each(|item| unsafe { buffer.push_unchecked(*item) });

        PrimitiveArray::<T>::from_data(array.data_type().clone(), buffer.into(), None)
    }
}

fn filter_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    mask: &BooleanArray,
) -> PrimitiveArray<T> {
    // todo: branch on mask.validity()
    filter_nonnull_primitive(array, mask.values())
}

fn filter_growable<'a>(growable: &mut impl Growable<'a>, chunks: &[(usize, usize)]) {
    chunks
        .iter()
        .for_each(|(start, len)| growable.extend(0, *start, *len));
}

macro_rules! dyn_build_filter {
    ($ty:ty, $array:expr, $filter_count:expr, $chunks:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        let mut growable = growable::GrowablePrimitive::<$ty>::new(&[array], false, $filter_count);
        filter_growable(&mut growable, &$chunks);
        let array: PrimitiveArray<$ty> = growable.into();
        Box::new(array)
    }};
}

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multi-column `RecordBatch`).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
pub fn build_filter(filter: &BooleanArray) -> Result<Filter> {
    let iter = SlicesIterator::new(filter.values());
    let filter_count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    Ok(Box::new(move |array: &dyn Array| match array.data_type() {
        DataType::UInt8 => {
            dyn_build_filter!(u8, array, filter_count, chunks)
        }
        DataType::UInt16 => {
            dyn_build_filter!(u16, array, filter_count, chunks)
        }
        DataType::UInt32 => {
            dyn_build_filter!(u32, array, filter_count, chunks)
        }
        DataType::UInt64 => {
            dyn_build_filter!(u64, array, filter_count, chunks)
        }
        DataType::Int8 => {
            dyn_build_filter!(i8, array, filter_count, chunks)
        }
        DataType::Int16 => {
            dyn_build_filter!(i16, array, filter_count, chunks)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_build_filter!(i32, array, filter_count, chunks)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            dyn_build_filter!(i64, array, filter_count, chunks)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_build_filter!(days_ms, array, filter_count, chunks)
        }
        DataType::Float32 => {
            dyn_build_filter!(f32, array, filter_count, chunks)
        }
        DataType::Float64 => {
            dyn_build_filter!(f64, array, filter_count, chunks)
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let mut growable = growable::GrowableUtf8::<i32>::new(&[array], false, filter_count);
            filter_growable(&mut growable, &chunks);
            let array: Utf8Array<i32> = growable.into();
            Box::new(array)
        }
        _ => {
            let mut mutable = make_growable(&[array], false, filter_count);
            chunks
                .iter()
                .for_each(|(start, len)| mutable.extend(0, *start, *len));
            mutable.as_box()
        }
    }))
}

/// Filters an [Array], returning elements matching the filter (i.e. where the values are true).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
/// # Example
/// ```rust
/// # use arrow2::array::{Int32Array, Primitive, BooleanArray};
/// # use arrow2::datatypes::DataType;
/// # use arrow2::error::Result;
/// # use arrow2::compute::filter::filter;
/// # fn main() -> Result<()> {
/// let array = Primitive::from_slice(&vec![5, 6, 7, 8, 9]).to(DataType::Int32);
/// let filter_array = BooleanArray::from_slice(&vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array)?;
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &Primitive::from_slice(vec![5, 8]).to(DataType::Int32));
/// # Ok(())
/// # }
/// ```
pub fn filter(array: &dyn Array, filter: &BooleanArray) -> Result<Box<dyn Array>> {
    match array.data_type() {
        DataType::UInt8 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<u8>(array, filter)))
        }
        DataType::UInt16 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<u16>(array, filter)))
        }
        DataType::UInt32 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<u32>(array, filter)))
        }
        DataType::UInt64 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<u64>(array, filter)))
        }
        DataType::Int8 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<i8>(array, filter)))
        }
        DataType::Int16 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<i16>(array, filter)))
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<i32>(array, filter)))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<i64>(array, filter)))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<days_ms>(array, filter)))
        }
        DataType::Float32 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<f32>(array, filter)))
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<f64>(array, filter)))
        }
        _ => {
            let iter = SlicesIterator::new(filter.values());
            let mut mutable = make_growable(&[array], false, iter.slots());
            iter.for_each(|(start, len)| mutable.extend(0, start, len));
            Ok(mutable.as_box())
        }
    }
}

/// Returns a new [RecordBatch] with arrays containing only values matching the filter.
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    filter: &BooleanArray,
) -> Result<RecordBatch> {
    let filter = build_filter(filter)?;
    let filtered_arrays = record_batch
        .columns()
        .iter()
        .map(|a| filter(a.as_ref()).into())
        .collect();
    RecordBatch::try_new(record_batch.schema().clone(), filtered_arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::DataType;

    #[test]
    fn test_filter_array_slice() {
        let a = Primitive::<i32>::from_slice(&[5, 6, 7, 8, 9])
            .to(DataType::Int32)
            .slice(1, 4);
        let b = BooleanArray::from_slice(vec![true, true, false, false, true]).slice(1, 4);
        let c = filter(&a, &b).unwrap();

        let expected = Primitive::<i32>::from_slice(&[6, 9]).to(DataType::Int32);

        assert_eq!(expected, c.as_ref());
    }

    #[test]
    fn test_filter_array_low_density() {
        // this test exercises the all 0's branch of the filter algorithm
        let mut data_values = (1..=65).collect::<Vec<i32>>();
        let mut filter_values = (1..=65).map(|i| matches!(i % 65, 0)).collect::<Vec<bool>>();
        // set up two more values after the batch
        data_values.extend_from_slice(&[66, 67]);
        filter_values.extend_from_slice(&[false, true]);
        let a = Primitive::<i32>::from_slice(data_values).to(DataType::Int32);
        let b = BooleanArray::from_slice(filter_values);
        let c = filter(&a, &b).unwrap();

        let expected = Primitive::<i32>::from_slice(&[65, 67]).to(DataType::Int32);

        assert_eq!(expected, c.as_ref());
    }

    #[test]
    fn test_filter_array_high_density() {
        // this test exercises the all 1's branch of the filter algorithm
        let mut data_values = (1..=65).map(Some).collect::<Vec<_>>();
        let mut filter_values = (1..=65)
            .map(|i| !matches!(i % 65, 0))
            .collect::<Vec<bool>>();
        // set second data value to null
        data_values[1] = None;
        // set up two more values after the batch
        data_values.extend_from_slice(&[Some(66), None, Some(67), None]);
        filter_values.extend_from_slice(&[false, true, true, true]);
        let a = Primitive::<i32>::from(data_values).to(DataType::Int32);
        let b = BooleanArray::from_slice(filter_values);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(67, d.len());
        assert_eq!(3, d.null_count());
        assert_eq!(1, d.value(0));
        assert_eq!(true, d.is_null(1));
        assert_eq!(64, d.value(63));
        assert_eq!(true, d.is_null(64));
        assert_eq!(67, d.value(65));
    }

    #[test]
    fn test_filter_string_array_simple() {
        let a = Utf8Array::<i32>::from_slice(vec!["hello", " ", "world", "!"]);
        let b = BooleanArray::from_slice(vec![true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
    }

    #[test]
    fn test_filter_primative_array_with_null() {
        let a = Primitive::<i32>::from(vec![Some(5), None]).to(DataType::Int32);
        let b = BooleanArray::from_slice(vec![false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, d.len());
        assert_eq!(true, d.is_null(0));
    }

    #[test]
    fn test_filter_string_array_with_null() {
        let a = Utf8Array::<i32>::from(&vec![Some("hello"), None, Some("world"), None]);
        let b = BooleanArray::from_slice(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!(false, d.is_null(0));
        assert_eq!(true, d.is_null(1));
    }

    #[test]
    fn test_filter_binary_array_with_null() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world"), None];
        let a = BinaryArray::<i32>::from(&data);
        let b = BooleanArray::from_slice(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap();
        assert_eq!(2, d.len());
        assert_eq!(b"hello", d.value(0));
        assert_eq!(false, d.is_null(0));
        assert_eq!(true, d.is_null(1));
    }

    /*
    #[test]
    fn test_filter_dictionary_array() {
        let values = vec![Some("hello"), None, Some("world"), Some("!")];
        let a: Int8DictionaryArray = values.iter().copied().collect();
        let b = BooleanArray::from(vec![false, true, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<Int8DictionaryArray>()
            .unwrap();
        let value_array = d.values();
        let values = value_array.as_any().downcast_ref::<StringArray>().unwrap();
        // values are cloned in the filtered dictionary array
        assert_eq!(3, values.len());
        // but keys are filtered
        assert_eq!(2, d.len());
        assert_eq!(true, d.is_null(0));
        assert_eq!("world", values.value(d.keys().value(1) as usize));
    }

    #[test]
    fn test_filter_list_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build();

        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 6, 8, 8]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00000111]))
            .build();

        //  a = [[0, 1, 2], [3, 4, 5], [6, 7], null]
        let a = LargeListArray::from(list_data);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let result = filter(&a, &b).unwrap();

        // expected: [[3, 4, 5], null]
        let value_data = ArrayData::builder(DataType::Int32)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&[3, 4, 5]))
            .build();

        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 3]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let expected = ArrayData::builder(list_data_type)
            .len(2)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00000001]))
            .build();

        assert_eq!(&make_array(expected), &result);
    }
    */
}
