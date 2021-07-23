use crate::{
    array::{Array, BooleanArray, Int32Array},
    buffer::MutableBuffer,
    datatypes::DataType,
};

use super::SortOptions;

/// Returns the indices that would sort a [`BooleanArray`].
pub fn sort_boolean(
    values: &BooleanArray,
    value_indices: Vec<i32>,
    null_indices: Vec<i32>,
    options: &SortOptions,
    limit: Option<usize>,
) -> Int32Array {
    let descending = options.descending;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(i32, bool)>>();

    let mut nulls = null_indices;

    if !descending {
        valids.sort_by(|a, b| a.1.cmp(&b.1));
    } else {
        valids.sort_by(|a, b| a.1.cmp(&b.1).reverse());
        // reverse to keep a stable ordering
        nulls.reverse();
    }

    let mut values = MutableBuffer::<i32>::with_capacity(values.len());

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
    }

    Int32Array::from_data(DataType::Int32, values.into(), None)
}
