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

use crate::{
    array::{Array, PrimitiveArray},
    buffer::MutableBuffer,
    datatypes::DataType,
    types::NativeType,
};

use super::super::SortOptions;

/// # Safety
/// `indices[i] < values.len()` for all i
#[inline]
unsafe fn sort_inner<T, F>(indices: &mut [i32], values: &[T], mut cmp: F, descending: bool)
where
    T: NativeType,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if descending {
        indices.sort_by(|lhs, rhs| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs).reverse()
        })
    } else {
        indices.sort_by(|lhs, rhs| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs)
        })
    }
}

pub fn indices_sorted_by<T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    options: &SortOptions,
) -> PrimitiveArray<i32>
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let descending = options.descending;
    let values = array.values();
    let validity = array.validity();

    if let Some(validity) = validity {
        let mut indices = MutableBuffer::<i32>::from_len_zeroed(array.len());

        if options.nulls_first {
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(0..array.len() as i32)
                .for_each(|(x, index)| {
                    if x {
                        indices[validity.null_count() + valids] = index;
                        valids += 1;
                    } else {
                        indices[nulls] = index;
                        nulls += 1;
                    }
                });
            // Soundness:
            // all indices in `indices` are by construction `< array.len() == values.len()`
            unsafe {
                sort_inner(
                    &mut indices.as_slice_mut()[validity.null_count()..],
                    values,
                    cmp,
                    options.descending,
                )
            }
        } else {
            let last_valid_index = array.len() - validity.null_count();
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(0..array.len() as i32)
                .for_each(|(x, index)| {
                    if x {
                        indices[valids] = index;
                        valids += 1;
                    } else {
                        indices[last_valid_index + nulls] = index;
                        nulls += 1;
                    }
                });

            // Soundness:
            // all indices in `indices` are by construction `< array.len() == values.len()`
            unsafe {
                sort_inner(
                    &mut indices.as_slice_mut()[..last_valid_index],
                    values,
                    cmp,
                    options.descending,
                )
            };
        }

        PrimitiveArray::<i32>::from_data(DataType::Int32, indices.into(), None)
    } else {
        let mut indices = unsafe { MutableBuffer::from_trusted_len_iter(0..values.len() as i32) };

        // Soundness:
        // indices are by construction `< values.len()`
        unsafe { sort_inner(&mut indices, values, cmp, descending) };

        PrimitiveArray::<i32>::from_data(DataType::Int32, indices.into(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ord;
    use crate::array::Primitive;

    fn test<T>(data: &[Option<T>], data_type: DataType, options: SortOptions, expected_data: &[i32])
    where
        T: NativeType + std::cmp::Ord,
    {
        let input = Primitive::<T>::from(data).to(data_type);
        let expected = Primitive::<i32>::from_slice(&expected_data).to(DataType::Int32);
        let output = indices_sorted_by(&input, ord::total_cmp, &options);
        assert_eq!(output, expected)
    }

    #[test]
    fn ascending_nulls_first() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            &[0, 5, 3, 1, 4, 2],
        );
    }

    #[test]
    fn ascending_nulls_last() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            &[3, 1, 4, 2, 0, 5],
        );
    }

    #[test]
    fn descending_nulls_first() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            &[0, 5, 2, 1, 4, 3],
        );
    }

    #[test]
    fn descending_nulls_last() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            &[2, 1, 4, 3, 0, 5],
        );
    }
}
