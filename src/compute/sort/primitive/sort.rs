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

use crate::buffer::MutableBuffer;
use crate::{
    array::{Array, PrimitiveArray},
    bits::SlicesIterator,
    buffer::MutableBitmap,
    types::NativeType,
};

use super::super::SortOptions;

fn sort_inner<T, F>(values: &mut [T], mut cmp: F, descending: bool)
where
    T: NativeType,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if descending {
        values.sort_unstable_by(|x, y| cmp(x, y).reverse());
    } else {
        values.sort_unstable_by(cmp);
    };
}

/// Sorts a [`PrimitiveArray`] according to `cmp` comparator and [`SortOptions`].
pub fn sort_by<T, F>(array: &PrimitiveArray<T>, cmp: F, options: &SortOptions) -> PrimitiveArray<T>
where
    T: NativeType,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    let values = array.values();
    let validity = array.validity();

    let (buffer, validity) = if let Some(validity) = validity {
        let nulls = (0..validity.null_count()).map(|_| false);
        let valids = (validity.null_count()..array.len()).map(|_| true);

        let mut buffer = MutableBuffer::<T>::with_capacity(array.len());
        let mut new_validity = MutableBitmap::with_capacity(array.len());
        let slices = SlicesIterator::new(validity);

        if options.nulls_first {
            nulls
                .chain(valids)
                .for_each(|value| unsafe { new_validity.push_unchecked(value) });
            (0..validity.null_count()).for_each(|_| buffer.push(T::default()));
            for (start, len) in slices {
                buffer.extend_from_slice(&values[start..start + len])
            }
            sort_inner(
                &mut buffer.as_slice_mut()[validity.null_count()..],
                cmp,
                options.descending,
            )
        } else {
            valids
                .chain(nulls)
                .for_each(|value| unsafe { new_validity.push_unchecked(value) });
            for (start, len) in slices {
                buffer.extend_from_slice(&values[start..start + len])
            }
            sort_inner(&mut buffer.as_slice_mut(), cmp, options.descending);

            (0..validity.null_count()).for_each(|_| buffer.push(T::default()));
        };

        (buffer, new_validity.into())
    } else {
        let mut buffer = MutableBuffer::<T>::new();
        buffer.extend_from_slice(values);

        sort_inner(&mut buffer.as_slice_mut(), cmp, options.descending);

        (buffer, None)
    };
    PrimitiveArray::<T>::from_data(array.data_type().clone(), buffer.into(), validity)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::ord;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    fn test_sort_primitive_arrays<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: SortOptions,
        expected_data: &[Option<T>],
    ) where
        T: NativeType + std::cmp::Ord,
    {
        let input = Primitive::<T>::from(data).to(data_type.clone());
        let expected = Primitive::<T>::from(expected_data).to(data_type);
        let output = sort_by(&input, ord::total_cmp, &options);
        assert_eq!(expected, output)
    }

    #[test]
    fn ascending_nulls_first() {
        test_sort_primitive_arrays::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            &[None, None, Some(2), Some(3), Some(3), Some(5)],
        );
    }

    #[test]
    fn ascending_nulls_last() {
        test_sort_primitive_arrays::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            &[Some(2), Some(3), Some(3), Some(5), None, None],
        );
    }

    #[test]
    fn descending_nulls_first() {
        test_sort_primitive_arrays::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            &[None, None, Some(5), Some(3), Some(3), Some(2)],
        );
    }

    #[test]
    fn descending_nulls_last() {
        test_sort_primitive_arrays::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            &[Some(5), Some(3), Some(3), Some(2), None, None],
        );
    }
}
