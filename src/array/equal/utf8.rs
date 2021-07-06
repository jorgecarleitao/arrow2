use crate::array::{Array, Offset, Utf8Array};

pub(super) fn equal<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}
