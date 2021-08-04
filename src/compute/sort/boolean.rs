use crate::{
    array::{Array, BooleanArray, Index, PrimitiveArray},
    buffer::MutableBuffer,
};

use super::SortOptions;

/// Returns the indices that would sort a [`BooleanArray`].
pub fn sort_boolean<I: Index>(
    values: &BooleanArray,
    value_indices: Vec<I>,
    null_indices: Vec<I>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let descending = options.descending;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index.to_usize())))
        .collect::<Vec<(I, bool)>>();

    let mut nulls = null_indices;

    if !descending {
        valids.sort_by(|a, b| a.1.cmp(&b.1));
    } else {
        valids.sort_by(|a, b| b.1.cmp(&a.1));
        // reverse to keep a stable ordering
        nulls.reverse();
    }

    let mut values = MutableBuffer::<I>::with_capacity(values.len());

    if options.nulls_first {
        values.extend_from_slice(nulls.as_slice());
        valids.iter().for_each(|x| values.push(x.0));
    } else {
        // nulls last
        valids.iter().for_each(|x| values.push(x.0));
        values.extend_from_slice(nulls.as_slice());
    }

    // un-efficient; there are much more performant ways of sorting nulls above, anyways.
    if let Some(limit) = limit {
        values.truncate(limit);
        values.shrink_to_fit();
    }

    PrimitiveArray::<I>::from_data(I::DATA_TYPE, values.into(), None)
}
