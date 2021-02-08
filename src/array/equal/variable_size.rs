use crate::{array::Offset, buffer::Bitmap};

use super::utils::{count_nulls, equal_len};

fn offset_value_equal<O: Offset>(
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_offsets: &[O],
    rhs_offsets: &[O],
    lhs_pos: usize,
    rhs_pos: usize,
    len: usize,
) -> bool {
    let lhs_start = lhs_offsets[lhs_pos].to_usize().unwrap();
    let rhs_start = rhs_offsets[rhs_pos].to_usize().unwrap();
    let lhs_len = lhs_offsets[lhs_pos + len] - lhs_offsets[lhs_pos];
    let rhs_len = rhs_offsets[rhs_pos + len] - rhs_offsets[rhs_pos];

    lhs_len == rhs_len
        && equal_len(
            lhs_values,
            rhs_values,
            lhs_start,
            rhs_start,
            lhs_len.to_usize().unwrap(),
        )
}

pub(super) fn equal<O: Offset>(
    lhs_offsets: &[O],
    rhs_offsets: &[O],
    lhs_values: &[u8],
    rhs_values: &[u8],
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    // the offsets of the `ArrayData` are ignored as they are only applied to the offset buffer.

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0
        && rhs_null_count == 0
        && !lhs_values.is_empty()
        && !rhs_values.is_empty()
    {
        offset_value_equal(
            lhs_values,
            rhs_values,
            lhs_offsets,
            rhs_offsets,
            lhs_start,
            rhs_start,
            len,
        )
    } else {
        let lhs_bitmap = lhs_nulls.as_ref().unwrap();
        let rhs_bitmap = rhs_nulls.as_ref().unwrap();

        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && offset_value_equal(
                        lhs_values,
                        rhs_values,
                        lhs_offsets,
                        rhs_offsets,
                        lhs_pos,
                        rhs_pos,
                        1,
                    )
        })
    }
}
