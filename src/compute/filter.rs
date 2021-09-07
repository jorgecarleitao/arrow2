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

use crate::array::growable::{make_growable, Growable};
use crate::bitmap::{utils::SlicesIterator, Bitmap, MutableBitmap};
use crate::record_batch::RecordBatch;
use crate::{array::*, types::NativeType};
use crate::{buffer::MutableBuffer, error::Result};

/// Function that can filter arbitrary arrays
pub type Filter<'a> = Box<dyn Fn(&dyn Array) -> Box<dyn Array> + 'a>;

fn filter_nonnull_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    mask: &Bitmap,
) -> PrimitiveArray<T> {
    assert_eq!(array.len(), mask.len());
    let filter_count = mask.len() - mask.null_count();

    let mut buffer = MutableBuffer::<T>::with_capacity(filter_count);
    if let Some(validity) = array.validity() {
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

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multi-column `RecordBatch`).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
pub fn build_filter(filter: &BooleanArray) -> Result<Filter> {
    let iter = SlicesIterator::new(filter.values());
    let filter_count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    use crate::datatypes::PhysicalType::*;
    Ok(Box::new(move |array: &dyn Array| {
        match array.data_type().to_physical_type() {
            Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
                let array = array.as_any().downcast_ref().unwrap();
                let mut growable =
                    growable::GrowablePrimitive::<$T>::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: PrimitiveArray<$T> = growable.into();
                Box::new(array)
            }),
            Utf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let mut growable = growable::GrowableUtf8::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: Utf8Array<i32> = growable.into();
                Box::new(array)
            }
            LargeUtf8 => {
                let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                let mut growable = growable::GrowableUtf8::new(vec![array], false, filter_count);
                filter_growable(&mut growable, &chunks);
                let array: Utf8Array<i64> = growable.into();
                Box::new(array)
            }
            _ => {
                let mut mutable = make_growable(&[array], false, filter_count);
                chunks
                    .iter()
                    .for_each(|(start, len)| mutable.extend(0, *start, *len));
                mutable.as_box()
            }
        }
    }))
}

/// Filters an [Array], returning elements matching the filter (i.e. where the values are true).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
/// # Example
/// ```rust
/// # use arrow2::array::{Int32Array, PrimitiveArray, BooleanArray};
/// # use arrow2::error::Result;
/// # use arrow2::compute::filter::filter;
/// # fn main() -> Result<()> {
/// let array = PrimitiveArray::from_slice([5, 6, 7, 8, 9]);
/// let filter_array = BooleanArray::from_slice(&vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array)?;
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &PrimitiveArray::from_slice(vec![5, 8]));
/// # Ok(())
/// # }
/// ```
pub fn filter(array: &dyn Array, filter: &BooleanArray) -> Result<Box<dyn Array>> {
    use crate::datatypes::PhysicalType::*;
    match array.data_type().to_physical_type() {
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            Ok(Box::new(filter_primitive::<$T>(array, filter)))
        }),
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
    filter_values: &BooleanArray,
) -> Result<RecordBatch> {
    let num_colums = record_batch.columns().len();

    let filtered_arrays = match num_colums {
        1 => {
            vec![filter(record_batch.columns()[0].as_ref(), filter_values)?.into()]
        }
        _ => {
            let filter = build_filter(filter_values)?;
            record_batch
                .columns()
                .iter()
                .map(|a| filter(a.as_ref()).into())
                .collect()
        }
    };
    RecordBatch::try_new(record_batch.schema().clone(), filtered_arrays)
}
