use crate::array::{Offset, StringSequenceArray};

pub(super) fn equal<O: Offset>(lhs: &StringSequenceArray<O>, rhs: &StringSequenceArray<O>) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}
