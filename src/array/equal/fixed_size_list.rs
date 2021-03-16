use crate::datatypes::DataType;
use crate::{
    array::{Array, FixedSizeListArray},
    bitmap::Bitmap,
};

use super::{
    equal_range,
    utils::{child_logical_null_buffer, count_validity},
};

pub(super) fn equal(
    lhs: &FixedSizeListArray,
    rhs: &FixedSizeListArray,
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let size = match lhs.data_type() {
        DataType::FixedSizeList(_, i) => *i as usize,
        _ => unreachable!(),
    };

    let lhs_values = lhs.values().as_ref();
    let rhs_values = rhs.values().as_ref();

    let child_lhs_validity = child_logical_null_buffer(lhs, lhs_validity, lhs_values);
    let child_rhs_validity = child_logical_null_buffer(rhs, rhs_validity, rhs_values);

    let lhs_null_count = count_validity(lhs_validity, lhs_start, len);
    let rhs_null_count = count_validity(rhs_validity, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        equal_range(
            lhs_values,
            rhs_values,
            &child_lhs_validity,
            &child_rhs_validity,
            size * lhs_start,
            size * rhs_start,
            size * len,
        )
    } else {
        let lhs_bitmap = lhs_validity.as_ref().unwrap();
        let rhs_bitmap = lhs_validity.as_ref().unwrap();

        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !lhs_bitmap.get_bit(lhs_pos);
            let rhs_is_null = !rhs_bitmap.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && equal_range(
                        lhs_values,
                        rhs_values,
                        &child_lhs_validity,
                        &child_rhs_validity,
                        lhs_pos * size,
                        rhs_pos * size,
                        size, // 1 * size since we are comparing a single entry
                    )
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{FixedSizeListPrimitive, Primitive};
    use crate::{array::equal::tests::test_equal, datatypes::DataType};

    use super::*;

    /// Create a fixed size list of 2 value lengths
    fn create_fixed_size_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> FixedSizeListArray {
        let data_type = FixedSizeListArray::default_datatype(DataType::Int32, 3);
        let list = data
            .as_ref()
            .iter()
            .map(|x| {
                Some(match x {
                    Some(x) => x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>(),
                    None => std::iter::repeat(None).take(3).collect::<Vec<_>>(),
                })
            })
            .collect::<FixedSizeListPrimitive<Primitive<i32>, i32>>();
        list.to(data_type)
    }

    #[test]
    fn test_fixed_size_list_equal() {
        let a = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(&a, &b, false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_fixed_list_null() {
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        /*
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(&a, &b, true);

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            Some(&[7, 8, 9]),
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);
         */

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);
    }

    #[test]
    fn test_fixed_list_offsets() {
        // Test the case where offset != 0
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);
    }
}
