use crate::{array::StructArray, buffer::Bitmap};

use super::{
    equal_range,
    utils::{child_logical_null_buffer, count_nulls},
};

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots. The null buffers `lhs_nulls` and `rhs_nulls` inherit parent nullability.
///
/// If an array is a child of a struct or list, the array's nulls have to be merged with the parent.
/// This then affects the null count of the array, thus the merged nulls are passed separately
/// as `lhs_nulls` and `rhs_nulls` variables to functions.
/// The nulls are merged with a bitwise AND, and null counts are recomputed where necessary.
fn equal_values(
    lhs: &StructArray,
    rhs: &StructArray,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    lhs.values()
        .iter()
        .zip(rhs.values())
        .all(|(lhs_values, rhs_values)| {
            // merge the null data
            let lhs_merged_nulls = child_logical_null_buffer(lhs, lhs_nulls, lhs_values.as_ref());
            let rhs_merged_nulls = child_logical_null_buffer(rhs, rhs_nulls, rhs_values.as_ref());
            equal_range(
                lhs_values.as_ref(),
                rhs_values.as_ref(),
                &lhs_merged_nulls,
                &rhs_merged_nulls,
                lhs_start,
                rhs_start,
                len,
            )
        })
}

pub(super) fn equal(
    lhs: &StructArray,
    rhs: &StructArray,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_bitmap = lhs_nulls.as_ref().unwrap();
        let rhs_bitmap = rhs_nulls.as_ref().unwrap();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            // if both struct and child had no null buffers,
            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_pos, rhs_pos, 1)
        })
    }
}
