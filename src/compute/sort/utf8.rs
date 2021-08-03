use crate::array::{Array, Index, Offset, PrimitiveArray, Utf8Array};
use crate::array::{DictionaryArray, DictionaryKey};

use super::common;
use super::SortOptions;

pub(super) fn indices_sorted_unstable_by<I: Index, O: Offset>(
    array: &Utf8Array<O>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let get = |idx| unsafe { array.value_unchecked(idx as usize) };
    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    common::indices_sorted_unstable_by(array.validity(), get, cmp, array.len(), options, limit)
}

pub(super) fn indices_sorted_unstable_by_dictionary<I: Index, K: DictionaryKey, O: Offset>(
    array: &DictionaryArray<K>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let keys = array.keys();

    let dict = array
        .values()
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .unwrap();

    let get = |idx| unsafe {
        let index = keys.value_unchecked(idx as usize);
        // Note: there is no check that the keys are within bounds of the dictionary.
        dict.value(index.to_usize().unwrap())
    };

    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    common::indices_sorted_unstable_by(array.validity(), get, cmp, array.len(), options, limit)
}
