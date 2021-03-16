use crate::{array::BooleanArray, bitmap::Bitmap};

use super::utils::{count_validity, equal_bits};

pub(super) fn equal(
    lhs: &BooleanArray,
    rhs: &BooleanArray,
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_null_count = count_validity(lhs_validity, lhs_start, len);
    let rhs_null_count = count_validity(rhs_validity, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_bits(lhs_values, rhs_values, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs_validity.as_ref().unwrap();
        let rhs_null_bytes = rhs_validity.as_ref().unwrap();

        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !lhs_null_bytes.get_bit(lhs_pos);
            let rhs_is_null = !rhs_null_bytes.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_bits(lhs_values, rhs_values, lhs_pos, rhs_pos, 1)
        })
    }
}
