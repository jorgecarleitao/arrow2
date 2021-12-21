use arrow2::array::*;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::datatypes::DataType;

#[test]
fn basic() {
    let a = MutableFixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        None,
    );
    assert_eq!(a.len(), 2);
    assert_eq!(a.data_type(), &DataType::FixedSizeBinary(2));
    assert_eq!(a.values(), &Vec::from([1, 2, 3, 4]));
    assert_eq!(a.validity(), None);
    assert_eq!(a.value(1), &[3, 4]);
    assert_eq!(unsafe { a.value_unchecked(1) }, &[3, 4]);
}

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let a = MutableFixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        None,
    );
    assert_eq!(a, a);
    let b = MutableFixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2]),
        None,
    );
    assert_eq!(b, b);
    assert!(a != b);
    let a = MutableFixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        Some(MutableBitmap::from([true, false])),
    );
    let b = MutableFixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        Some(MutableBitmap::from([false, true])),
    );
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
}

#[test]
fn try_from_iter() {
    let array = MutableFixedSizeBinaryArray::try_from_iter(
        vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")],
        2,
    )
    .unwrap();
    assert_eq!(array.len(), 4);
}

#[test]
fn push_null() {
    let mut array = MutableFixedSizeBinaryArray::new(2);
    array.push::<&[u8]>(None);

    let array: FixedSizeBinaryArray = array.into();
    assert_eq!(array.validity(), Some(&Bitmap::from([false])));
}
