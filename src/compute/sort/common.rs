use crate::{
    array::{Index, PrimitiveArray},
    bitmap::Bitmap,
    buffer::MutableBuffer,
};

use super::SortOptions;

/// # Safety
/// This function guarantees that:
/// * `get` is only called for `0 <= i < limit`
/// * `cmp` is only called from the co-domain of `get`.
#[inline]
fn k_element_sort_inner<I: Index, T, F>(
    indices: &mut [I],
    values: &[T],
    descending: bool,
    limit: usize,
    mut cmp: F,
) where
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if descending {
        let mut compare = |lhs: &I, rhs: &I| unsafe {
            let lhs = values.get_unchecked(lhs.to_usize());
            let rhs = values.get_unchecked(rhs.to_usize());
            cmp(rhs, lhs)
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, &mut compare);
        before.sort_unstable_by(&mut compare);
    } else {
        let mut compare = |lhs: &I, rhs: &I| unsafe {
            let lhs = values.get_unchecked(lhs.to_usize());
            let rhs = values.get_unchecked(rhs.to_usize());
            cmp(lhs, rhs)
        };
        let (before, _, _) = indices.select_nth_unstable_by(limit, &mut compare);
        before.sort_unstable_by(&mut compare);
    }
}

/// # Safety
/// This function guarantees that:
/// * `get` is only called for `0 <= i < limit`
/// * `cmp` is only called from the co-domain of `get`.
#[inline]
fn sort_unstable_by<I: Index, T, F>(
    indices: &mut [I],
    values: &[T],
    mut cmp: F,
    descending: bool,
    limit: usize,
) where
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    if limit != indices.len() {
        return k_element_sort_inner(indices, values, descending, limit, cmp);
    }

    if descending {
        indices.sort_unstable_by(|lhs, rhs| unsafe {
            let lhs = values.get_unchecked(lhs.to_usize());
            let rhs = values.get_unchecked(rhs.to_usize());
            cmp(rhs, lhs)
        })
    } else {
        indices.sort_unstable_by(|lhs, rhs| unsafe {
            let lhs = values.get_unchecked(lhs.to_usize());
            let rhs = values.get_unchecked(rhs.to_usize());
            cmp(lhs, rhs)
        })
    }
}

/// # Safety
/// This function guarantees that:
/// * `get` is only called for `0 <= i < length`
/// * `cmp` is only called from the co-domain of `get`.
#[inline]
pub(super) fn indices_sorted_unstable_by<I, T, F>(
    validity: &Option<Bitmap>,
    values: &[T],
    cmp: F,
    length: usize,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I>
where
    I: Index,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let descending = options.descending;

    let limit = limit.unwrap_or(length);
    // Safety: without this, we go out of bounds when limit >= length.
    let limit = limit.min(length);

    let indices = if let Some(validity) = validity {
        let mut indices = MutableBuffer::<I>::with_capacity(length);
        unsafe {
            indices.set_len(length);
        }
        if options.nulls_first {
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(0..length)
                .for_each(|(is_valid, index)| {
                    if is_valid {
                        indices[validity.null_count() + valids] = I::from_usize(index).unwrap();
                        valids += 1;
                    } else {
                        indices[nulls] = I::from_usize(index).unwrap();
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
                sort_unstable_by(indices, values, cmp, options.descending, limit)
            }
        } else {
            let last_valid_index = length - validity.null_count();
            let mut nulls = 0;
            let mut valids = 0;
            validity.iter().zip(0..length).for_each(|(x, index)| {
                if x {
                    indices[valids] = I::from_usize(index).unwrap();
                    valids += 1;
                } else {
                    indices[last_valid_index + nulls] = I::from_usize(index).unwrap();
                    nulls += 1;
                }
            });

            // Soundness:
            // all indices in `indices` are by construction `< array.len() == values.len()`
            // limit is by construction <= values.len()
            let limit = limit.min(last_valid_index);
            let indices = &mut indices.as_mut_slice()[..last_valid_index];
            sort_unstable_by(indices, values, cmp, options.descending, limit);
        }

        indices.truncate(limit);
        indices.shrink_to_fit();

        indices
    } else {
        let mut indices = unsafe {
            MutableBuffer::from_trusted_len_iter_unchecked(
                I::from_usize(0).unwrap()..I::from_usize(length).unwrap(),
            )
        };
        // Soundness:
        // indices are by construction `< values.len()`
        // limit is by construction `< values.len()`
        sort_unstable_by(&mut indices, values, cmp, descending, limit);
        indices.truncate(limit);
        indices.shrink_to_fit();
        indices
    };

    PrimitiveArray::<I>::from_data(I::DATA_TYPE, indices.into(), None)
}
