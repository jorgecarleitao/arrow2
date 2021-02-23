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

use std::sync::Arc;

use crate::{
    array::{Array, ListArray, Offset},
    buffer::{MutableBitmap, MutableBuffer},
};

use super::{
    make_growable,
    utils::{build_extend_null_bits, extend_offsets, ExtendNullBits},
    Growable,
};

fn extend_offset_values<'a, O: Offset>(
    growable: &mut GrowableList<'a, O>,
    index: usize,
    start: usize,
    len: usize,
) {
    let array = growable.arrays[index];
    let offsets = array.offsets();

    if array.null_count() == 0 {
        // offsets
        extend_offsets::<O>(
            &mut growable.offsets,
            &mut growable.last_offset,
            &offsets[start..start + len + 1],
        );

        let end = offsets[start + len].to_usize().unwrap();
        let start = offsets[start].to_usize().unwrap();
        let len = end - start;
        growable.values.extend(index, start, len)
    } else {
        growable.offsets.reserve(len);

        let new_offsets = &mut growable.offsets;
        let inner_values = &mut growable.values;
        let last_offset = &mut growable.last_offset;
        (start..start + len).for_each(|i| {
            if array.is_valid(i) {
                let len = offsets[i + 1] - offsets[i];
                // compute the new offset
                *last_offset += len;

                // append value
                inner_values.extend(
                    index,
                    offsets[i].to_usize().unwrap(),
                    len.to_usize().unwrap(),
                );
            }
            // append offset
            new_offsets.push(*last_offset);
        })
    }
}

pub struct GrowableList<'a, O: Offset> {
    arrays: Vec<&'a ListArray<O>>,
    validity: MutableBitmap,
    values: Box<dyn Growable<'a> + 'a>,
    offsets: MutableBuffer<O>,
    last_offset: O, // always equal to the last offset at `offsets`.
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a, O: Offset> GrowableList<'a, O> {
    /// # Panics
    /// This function panics if any of the `arrays` is not downcastable to `PrimitiveArray<T>`.
    pub fn new(arrays: &[&'a dyn Array], mut use_nulls: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_nulls = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_nulls))
            .collect();

        let arrays = arrays
            .iter()
            .map(|array| array.as_any().downcast_ref::<ListArray<O>>().unwrap())
            .collect::<Vec<_>>();

        let inner = arrays
            .iter()
            .map(|array| array.values().as_ref())
            .collect::<Vec<_>>();
        let values = make_growable(&inner, use_nulls, 0);

        let mut offsets = MutableBuffer::with_capacity(capacity + 1);
        let length = O::default();
        unsafe { offsets.push_unchecked(length) };

        Self {
            arrays,
            offsets,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            last_offset: O::default(),
            extend_null_bits,
        }
    }
}

impl<'a, O: Offset> Growable<'a> for GrowableList<'a, O> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);
        extend_offset_values::<O>(self, index, start, len);
    }

    fn extend_nulls(&mut self, additional: usize) {
        (0..additional).for_each(|_| self.offsets.push(self.last_offset));
        self.validity.extend_constant(additional, false);
    }

    fn to_arc(&mut self) -> Arc<dyn Array> {
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = self.values.to_arc();

        Arc::new(ListArray::<O>::from_data(
            self.arrays[0].data_type().clone(),
            offsets.into(),
            values,
            validity.into(),
        ))
    }
}

impl<'a, O: Offset> Into<ListArray<O>> for GrowableList<'a, O> {
    fn into(self) -> ListArray<O> {
        let mut values = self.values;
        let values = values.to_arc();

        ListArray::<O>::from_data(
            self.arrays[0].data_type().clone(),
            self.offsets.into(),
            values,
            self.validity.into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{ListPrimitive, Primitive};
    use crate::{array::ListArray, datatypes::DataType};

    #[test]
    fn basic() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(6i32), Some(7), Some(8)]),
        ];

        let array: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let array = array.to(ListArray::<i32>::default_datatype(DataType::Int32));

        let mut a = GrowableList::new(&[&array], false, 0);
        a.extend(0, 0, 1);

        let result: ListArray<i32> = a.into();

        let expected = vec![Some(vec![Some(1i32), Some(2), Some(3)])];
        let expected: ListPrimitive<i32, Primitive<i32>, i32> = expected.into_iter().collect();
        let expected = expected.to(ListArray::<i32>::default_datatype(DataType::Int32));

        assert_eq!(result, expected)
    }

    #[test]
    fn null_offset() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(6i32), Some(7), Some(8)]),
        ];
        let array: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let array = array.to(ListArray::<i32>::default_datatype(DataType::Int32));
        let array = array.slice(1, 2);

        let mut a = GrowableList::new(&[&array], false, 0);
        a.extend(0, 1, 1);

        let result: ListArray<i32> = a.into();

        let expected = vec![Some(vec![Some(6i32), Some(7), Some(8)])];
        let expected: ListPrimitive<i32, Primitive<i32>, i32> = expected.into_iter().collect();
        let expected = expected.to(ListArray::<i32>::default_datatype(DataType::Int32));

        assert_eq!(result, expected)
    }

    #[test]
    fn null_offsets() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(6i32), None, Some(8)]),
        ];
        let array: ListPrimitive<i32, Primitive<i32>, i32> = data.into_iter().collect();
        let array = array.to(ListArray::<i32>::default_datatype(DataType::Int32));
        let array = array.slice(1, 2);

        let mut a = GrowableList::new(&[&array], false, 0);
        a.extend(0, 1, 1);

        let result: ListArray<i32> = a.into();

        let expected = vec![Some(vec![Some(6i32), None, Some(8)])];
        let expected: ListPrimitive<i32, Primitive<i32>, i32> = expected.into_iter().collect();
        let expected = expected.to(ListArray::<i32>::default_datatype(DataType::Int32));

        assert_eq!(result, expected)
    }
}
