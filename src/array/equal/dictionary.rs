use crate::{
    array::{DictionaryArray, DictionaryKey},
    bitmap::Bitmap,
};

use super::equal as _equal;
use super::utils::count_validity;

pub(super) fn equal<K: DictionaryKey>(
    lhs: &DictionaryArray<K>,
    rhs: &DictionaryArray<K>,
    lhs_validity: &Option<Bitmap>,
    rhs_validity: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    let lhs_keys = lhs.keys();
    let rhs_keys = rhs.keys();
    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_null_count = count_validity(lhs_validity, lhs_start, len);
    let rhs_null_count = count_validity(rhs_validity, rhs_start, len);

    if lhs_null_count == 0 && rhs_null_count == 0 {
        lhs_keys
            .iter()
            .zip(rhs_keys.iter())
            .all(|(lhs, rhs)| match (lhs, rhs) {
                (None, None) => true,
                (Some(_), None) => false,
                (None, Some(_)) => false,
                (Some(l), Some(r)) => {
                    let lhs = lhs_values.slice(l.to_usize().unwrap(), 1);
                    let rhs = rhs_values.slice(r.to_usize().unwrap(), 1);
                    _equal(lhs.as_ref(), rhs.as_ref())
                }
            })
    } else {
        let lhs_bitmap = lhs_validity.as_ref().unwrap();
        let rhs_bitmap = rhs_validity.as_ref().unwrap();
        let lhs_bitmap_iter = lhs_bitmap.iter();
        let rhs_bitmap_iter = rhs_bitmap.iter();
        let keys_iter = lhs_keys.iter().zip(rhs_keys.iter());
        let bitmap_iter = lhs_bitmap_iter.zip(rhs_bitmap_iter);
        let mut iter = keys_iter.zip(bitmap_iter).skip(lhs_start).take(len);
        iter.all(|((lhs, rhs), (bit1, bit2))| {
            if !bit1 && !bit2 {
                return true;
            };
            if bit1 != bit2 {
                return false;
            };
            match (lhs, rhs) {
                (None, None) => true,
                (Some(_), None) => false,
                (None, Some(_)) => false,
                (Some(l), Some(r)) => {
                    let lhs = lhs_values.slice(l.to_usize().unwrap(), 1);
                    let rhs = rhs_values.slice(r.to_usize().unwrap(), 1);
                    _equal(lhs.as_ref(), rhs.as_ref())
                }
            }
        })
    }
}
