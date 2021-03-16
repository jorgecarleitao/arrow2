use crate::{array::FixedSizeBinaryArray, bitmap::Bitmap};

use super::utils::{count_validity, equal_len};

pub(super) fn equal(
    lhs: &FixedSizeBinaryArray,
    rhs: &FixedSizeBinaryArray,
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let size = lhs.size();
    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_null_count = count_validity(lhs_validity, lhs_start, len);
    let rhs_null_count = count_validity(rhs_validity, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_len(
            lhs_values,
            rhs_values,
            size * lhs_start,
            size * rhs_start,
            size * len,
        )
    } else {
        let lhs_bitmap = lhs_validity.as_ref().unwrap();
        let rhs_bitmap = rhs_validity.as_ref().unwrap();

        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_len(
                        lhs_values,
                        rhs_values,
                        lhs_pos * size,
                        rhs_pos * size,
                        size, // 1 * size since we are comparing a single entry
                    )
        })
    }
}
