use arrow2::{
    array::{MutableArray, MutableBooleanArray},
    bitmap::MutableBitmap,
};

#[test]
fn push() {
    let mut a = MutableBooleanArray::new();
    a.push(Some(true));
    a.push(None);
    assert_eq!(a.len(), 2);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));

    assert_eq!(a.values(), &MutableBitmap::from([true, false]));
}

#[test]
fn from_trusted_len_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a = MutableBooleanArray::from_trusted_len_iter(iter);
    assert_eq!(a.len(), 2);
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a: MutableBooleanArray = iter.collect();
    assert_eq!(a.len(), 2);
}
