use crate::{
    array::primitive::PrimitiveArray,
    buffer::{types::NativeType, Bitmap},
};

use super::utils::{count_nulls, equal_len};

pub(super) fn equal<T: NativeType>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_values = &lhs.values();
    let rhs_values = &rhs.values();

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        // without nulls, we just need to compare slices
        equal_len(lhs_values, rhs_values, lhs_start, rhs_start, len)
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_bitmap = lhs_nulls.as_ref().unwrap();
        let rhs_bitmap = rhs_nulls.as_ref().unwrap();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_len(lhs_values, rhs_values, lhs_pos, rhs_pos, 1)
        })
    }
}
