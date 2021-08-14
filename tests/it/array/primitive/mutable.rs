use arrow2::{
    array::*,
    bitmap::{Bitmap, MutableBitmap},
    buffer::MutableBuffer,
};
use std::iter::FromIterator;

#[test]
fn push() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(Some(1));
    a.push(None);
    assert_eq!(a.len(), 2);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));

    assert_eq!(a.values(), &MutableBuffer::from([1, 0]));
}

#[test]
fn set() {
    let mut a = MutablePrimitiveArray::<i32>::from([Some(1), None]);

    a.set(0, Some(2));
    a.set(1, Some(1));

    assert_eq!(a.len(), 2);
    assert!(a.is_valid(0));
    assert!(a.is_valid(1));

    assert_eq!(a.values(), &MutableBuffer::from([2, 1]));
}

#[test]
fn from_iter() {
    let a = MutablePrimitiveArray::<i32>::from_iter((0..2).map(Some));
    assert_eq!(a.len(), 2);
    assert_eq!(a.validity(), &None);
}

#[test]
fn natural_arc() {
    let a = MutablePrimitiveArray::<i32>::from_slice(&[0, 1]).into_arc();
    assert_eq!(a.len(), 2);
}

#[test]
fn only_nulls() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.push(None);
    a.push(None);
    let a: PrimitiveArray<i32> = a.into();
    assert_eq!(a.validity(), &Some(Bitmap::from([false, false])));
}

#[test]
fn from_trusted_len() {
    let a = MutablePrimitiveArray::<i32>::from_trusted_len_iter(vec![Some(1), None].into_iter());
    let a: PrimitiveArray<i32> = a.into();
    assert_eq!(a.validity(), &Some(Bitmap::from([true, false])));
}

#[test]
fn extend_trusted_len() {
    let mut a = MutablePrimitiveArray::<i32>::new();
    a.extend_trusted_len(vec![Some(1), Some(2)].into_iter());
    assert_eq!(a.validity(), &None);
    a.extend_trusted_len(vec![None, Some(4)].into_iter());
    assert_eq!(
        a.validity(),
        &Some(MutableBitmap::from([true, true, false, true]))
    );
    assert_eq!(a.values(), &MutableBuffer::<i32>::from([1, 2, 0, 4]));
}
