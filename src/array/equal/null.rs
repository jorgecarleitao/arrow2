use crate::array::NullArray;

#[inline]
pub(super) fn equal(_lhs: &NullArray, _rhs: &NullArray) -> bool {
    // a null buffer's range is always true, as every element is by definition equal (to null).
    // We only need to compare data_types
    true
}
