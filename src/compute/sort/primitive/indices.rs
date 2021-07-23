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
unsafe fn k_element_sort_inner<T, F>(
    indices: &mut [i32],
    values: &[T],
    descending: bool,
    limit: usize,
    mut cmp: F,
) where
    T: NativeType,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if descending {
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs).reverse()
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, compare);
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs).reverse()
        };
        before.sort_unstable_by(compare);
    } else {
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs)
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, compare);
        let compare = |lhs: &i32, rhs: &i32| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs)
        };
        before.sort_unstable_by(compare);
    }
}

/// # Safety
/// Safe iff
/// * `indices[i] < values.len()` for all i
/// * `limit < values.len()`
#[inline]
unsafe fn sort_unstable_by<T, F>(
    indices: &mut [i32],
    values: &[T],
    mut cmp: F,
    descending: bool,
    limit: usize,
) where
    T: NativeType,
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if limit != indices.len() {
        return k_element_sort_inner(indices, values, descending, limit, cmp);
    }

    if descending {
        indices.sort_unstable_by(|lhs, rhs| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs).reverse()
        })
    } else {
        indices.sort_unstable_by(|lhs, rhs| {
            let lhs = values.get_unchecked(*lhs as usize);
            let rhs = values.get_unchecked(*rhs as usize);
            cmp(lhs, rhs)
        })
    }
}

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<i32>
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let descending = options.descending;
    let values = array.values();
    let validity = array.validity();

    let limit = limit.unwrap_or_else(|| array.len());
    // Safety: without this, we go out of bounds when limit >= array.len().
    let limit = limit.min(array.len());

    let indices = if let Some(validity) = validity {
        let mut indices = MutableBuffer::<i32>::from_len_zeroed(array.len());

        if options.nulls_first {
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(0..array.len() as i32)
                .for_each(|(is_valid, index)| {
                    if is_valid {
                        indices[validity.null_count() + valids] = index;
                        valids += 1;
                    } else {
                        indices[nulls] = index;
                        nulls += 1;
                    }
                });

            if limit > validity.null_count() {
                // when limit is larger, we must sort values:

                // Soundness:
                // all indices in `indices` are by construction `< array.len() == values.len()`
                // limit is by construction < indices.len()
                let limit = limit - validity.null_count();
                let indices = &mut indices.as_mut_slice()[validity.null_count()..];
                unsafe { sort_unstable_by(indices, values, cmp, options.descending, limit) }
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
            // limit is by construction <= values.len()
            let limit = limit.min(last_valid_index);
            let indices = &mut indices.as_mut_slice()[..last_valid_index];
            unsafe { sort_unstable_by(indices, values, cmp, options.descending, limit) };
        }

        indices.truncate(limit);
        indices.shrink_to_fit();

        indices
    } else {
        let mut indices =
            unsafe { MutableBuffer::from_trusted_len_iter_unchecked(0..values.len() as i32) };

        // Soundness:
        // indices are by construction `< values.len()`
        // limit is by construction `< values.len()`
        unsafe { sort_unstable_by(&mut indices, values, cmp, descending, limit) };

        indices.truncate(limit);
        indices.shrink_to_fit();

        indices
    };
    PrimitiveArray::<i32>::from_data(DataType::Int32, indices.into(), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ord;
    use crate::array::*;

    fn test<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: SortOptions,
        limit: Option<usize>,
        expected_data: &[i32],
    ) where
        T: NativeType + std::cmp::Ord,
    {
        let input = PrimitiveArray::<T>::from(data).to(data_type);
        let expected = Int32Array::from_slice(&expected_data);
        let output = indices_sorted_unstable_by(&input, ord::total_cmp, &options, limit);
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
            None,
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
            None,
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
            None,
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
            None,
            &[2, 1, 4, 3, 0, 5],
        );
    }

    #[test]
    fn limit_ascending_nulls_first() {
        // nulls sorted
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            Some(2),
            &[0, 5],
        );

        // nulls and values sorted
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            Some(4),
            &[0, 5, 3, 1],
        );
    }

    #[test]
    fn limit_ascending_nulls_last() {
        // values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            Some(2),
            &[3, 1],
        );

        // values and nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            Some(5),
            &[3, 1, 4, 2, 0],
        );
    }

    #[test]
    fn limit_descending_nulls_first() {
        // nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(2),
            &[0, 5],
        );

        // nulls and values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(4),
            &[0, 5, 2, 1],
        );
    }

    #[test]
    fn limit_descending_nulls_last() {
        // values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(2),
            &[2, 1],
        );

        // values and nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(5),
            &[2, 1, 4, 3, 0],
        );
    }
}
