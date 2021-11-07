use crate::array::{Array, DictionaryArray, DictionaryKey};

pub(super) fn equal<K: DictionaryKey>(lhs: &DictionaryArray<K>, rhs: &DictionaryArray<K>) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}
