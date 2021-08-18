use arrow2::array::{MutableArray, MutableBooleanArray};

#[test]
fn set() {
    let mut a = MutableBooleanArray::from(&[Some(false), Some(true), Some(false)]);

    a.set(1, None);
    a.set(0, Some(true));
    assert_eq!(
        a,
        MutableBooleanArray::from([Some(true), None, Some(false)])
    )
}

#[test]
fn push() {
    let mut a = MutableBooleanArray::new();
    a.push(Some(true));
    a.push(Some(false));
    a.push(None);
    a.push_null();
    assert_eq!(
        a,
        MutableBooleanArray::from([Some(true), Some(false), None, None])
    );
}

#[test]
fn from_trusted_len_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a = MutableBooleanArray::from_trusted_len_iter(iter);
    assert_eq!(a, MutableBooleanArray::from([Some(true), Some(true)]));
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a: MutableBooleanArray = iter.collect();
    assert_eq!(a, MutableBooleanArray::from([Some(true), Some(true)]));
}
