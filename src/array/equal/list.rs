use crate::{
    array::{Array, ListArray, Offset},
    buffer::Bitmap,
};

use super::{
    equal_range,
    utils::{child_logical_null_buffer, count_nulls},
};

fn lengths_equal<O: Offset>(lhs: &[O], rhs: &[O]) -> bool {
    // invariant from `base_equal`
    debug_assert_eq!(lhs.len(), rhs.len());

    if lhs.is_empty() {
        return true;
    }

    if lhs[0] == O::zero() && rhs[0] == O::zero() {
        return lhs == rhs;
    };

    // The expensive case, e.g.
    // [0, 2, 4, 6, 9] == [4, 6, 8, 10, 13]
    lhs.windows(2)
        .zip(rhs.windows(2))
        .all(|(lhs_offsets, rhs_offsets)| {
            // length of left == length of right
            (lhs_offsets[1] - lhs_offsets[0]) == (rhs_offsets[1] - rhs_offsets[0])
        })
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn offset_value_equal<O: Offset>(
    lhs_values: &dyn Array,
    rhs_values: &dyn Array,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
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
        && equal_range(
            lhs_values,
            rhs_values,
            lhs_nulls,
            rhs_nulls,
            lhs_start,
            rhs_start,
            lhs_len.to_usize().unwrap(),
        )
}

pub(super) fn equal<O: Offset>(
    lhs: &ListArray<O>,
    rhs: &ListArray<O>,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_offsets = lhs.offsets();
    let rhs_offsets = rhs.offsets();

    // There is an edge-case where a n-length list that has 0 children, results in panics.
    // For example; an array with offsets [0, 0, 0, 0, 0] has 4 slots, but will have
    // no valid children.
    // Under logical equality, the child null bitmap will be an empty buffer, as there are
    // no child values. This causes panics when trying to count set bits.
    //
    // We caught this by chance from an accidental test-case, but due to the nature of this
    // crash only occuring on list equality checks, we are adding a check here, instead of
    // on the buffer/bitmap utilities, as a length check would incur a penalty for almost all
    // other use-cases.
    //
    // The solution is to check the number of child values from offsets, and return `true` if
    // they = 0. Empty arrays are equal, so this is correct.
    //
    // It's unlikely that one would create a n-length list array with no values, where n > 0,
    // however, one is more likely to slice into a list array and get a region that has 0
    // child values.
    // The test that triggered this behaviour had [4, 4] as a slice of 1 value slot.
    let lhs_child_length = lhs_offsets.get(len).unwrap().to_usize().unwrap()
        - lhs_offsets.first().unwrap().to_usize().unwrap();
    let rhs_child_length = rhs_offsets.get(len).unwrap().to_usize().unwrap()
        - rhs_offsets.first().unwrap().to_usize().unwrap();

    if lhs_child_length == 0 && lhs_child_length == rhs_child_length {
        return true;
    }

    let lhs_values = lhs.values().as_ref();
    let rhs_values = rhs.values().as_ref();

    let lhs_null_count = count_nulls(lhs_nulls, lhs_start, len);
    let rhs_null_count = count_nulls(rhs_nulls, rhs_start, len);

    // compute the child logical bitmap
    let child_lhs_nulls = child_logical_null_buffer(lhs, lhs_nulls, lhs_values);
    let child_rhs_nulls = child_logical_null_buffer(rhs, rhs_nulls, rhs_values);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        lengths_equal(
            &lhs_offsets[lhs_start..lhs_start + len],
            &rhs_offsets[rhs_start..rhs_start + len],
        ) && equal_range(
            lhs_values,
            rhs_values,
            &child_lhs_nulls,
            &child_rhs_nulls,
            lhs_offsets[lhs_start].to_usize().unwrap(),
            rhs_offsets[rhs_start].to_usize().unwrap(),
            (lhs_offsets[len] - lhs_offsets[lhs_start])
                .to_usize()
                .unwrap(),
        )
    } else {
        // get a ref of the parent null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs_nulls.as_ref().unwrap();
        let rhs_null_bytes = rhs_nulls.as_ref().unwrap();
        // with nulls, we need to compare item by item whenever it is not null
        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;

            let lhs_is_null = !lhs_null_bytes.get_bit(lhs_pos);
            let rhs_is_null = !rhs_null_bytes.get_bit(rhs_pos);

            lhs_is_null
                || (lhs_is_null == rhs_is_null)
                    && offset_value_equal::<O>(
                        lhs_values,
                        rhs_values,
                        &child_lhs_nulls,
                        &child_rhs_nulls,
                        lhs_offsets,
                        rhs_offsets,
                        lhs_pos,
                        rhs_pos,
                        1,
                    )
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{ListPrimitive, Primitive};
    use crate::{array::equal::tests::test_equal, datatypes::DataType};

    use super::*;

    fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let list = data
            .as_ref()
            .into_iter()
            .map(|x| {
                x.as_ref()
                    .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
            })
            .collect::<ListPrimitive<i32, Primitive<i32>, i32>>();
        list.to(data_type)
    }

    #[test]
    fn test_list_equal() {
        let a = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(&a, &b, true);

        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(&a, &b, false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_list_null() {
        let a = create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b = create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        test_equal(&a, &b, true);

        let b = create_list_array(&[
            Some(&[1, 2]),
            None,
            Some(&[5, 6]),
            Some(&[3, 4]),
            None,
            None,
        ]);
        test_equal(&a, &b, false);

        let b = create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);
        test_equal(&a, &b, false);
    }

    // Test the case where offset != 0
    #[test]
    fn test_list_offsets() {
        let a = create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b = create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);

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
