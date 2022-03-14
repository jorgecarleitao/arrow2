use arrow2::{array::FixedSizeBinaryArray, bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

mod mutable;

#[test]
fn basics() {
    let array = FixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        Buffer::from_slice([1, 2, 3, 4, 5, 6]),
        Some(Bitmap::from([true, false, true])),
    );
    assert_eq!(array.size(), 2);
    assert_eq!(array.len(), 3);
    assert_eq!(array.validity(), Some(&Bitmap::from([true, false, true])));

    assert_eq!(array.value(0), [1, 2]);
    assert_eq!(array.value(2), [5, 6]);

    let array = array.slice(1, 2);

    assert_eq!(array.value(1), [5, 6]);
}

#[test]
fn with_validity() {
    let values = Buffer::from_slice([1, 2, 3, 4, 5, 6]);
    let a = FixedSizeBinaryArray::from_data(DataType::FixedSizeBinary(2), values, None);
    let a = a.with_validity(Some(Bitmap::from([true, false, true])));
    assert!(a.validity().is_some());
}

#[test]
fn debug() {
    let values = Buffer::from_slice([1, 2, 3, 4, 5, 6]);
    let a = FixedSizeBinaryArray::from_data(
        DataType::FixedSizeBinary(2),
        values,
        Some(Bitmap::from([true, false, true])),
    );
    assert_eq!(
        format!("{:?}", a),
        "FixedSizeBinary(2)[[1, 2], None, [5, 6]]"
    );
}

#[test]
fn empty() {
    let array = FixedSizeBinaryArray::new_empty(DataType::FixedSizeBinary(2));
    assert_eq!(array.values().len(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn null() {
    let array = FixedSizeBinaryArray::new_null(DataType::FixedSizeBinary(2), 2);
    assert_eq!(array.values().len(), 4);
    assert_eq!(array.validity().cloned(), Some([false, false].into()));
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(vec![1u8, 2]).take(2).map(Some);
    let a = FixedSizeBinaryArray::from_iter(iter, 2);
    assert_eq!(a.len(), 2);
}
