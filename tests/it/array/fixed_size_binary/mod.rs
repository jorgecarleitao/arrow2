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
    let a = FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), values, None);
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
fn from_iter() {
    let iter = std::iter::repeat(vec![1u8, 2]).take(2).map(Some);
    let a = FixedSizeBinaryArray::from_iter(iter, 2);
    assert_eq!(a.len(), 2);
}

#[test]
fn wrong_size() {
    let values = Buffer::from_slice(b"abb");
    assert!(FixedSizeBinaryArray::try_new(DataType::FixedSizeBinary(2), values, None).is_err());
}

#[test]
fn wrong_len() {
    let values = Buffer::from_slice(b"abba");
    let validity = Some([true, false, false].into()); // it should be 2
    assert!(FixedSizeBinaryArray::try_new(DataType::FixedSizeBinary(2), values, validity).is_err());
}

#[test]
fn wrong_data_type() {
    let values = Buffer::from_slice(b"abba");
    assert!(FixedSizeBinaryArray::try_new(DataType::Binary, values, None).is_err());
}

#[test]
fn to() {
    let values = Buffer::from_slice(b"abba");
    let a = FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), values, None);

    let extension = DataType::Extension(
        "a".to_string(),
        Box::new(DataType::FixedSizeBinary(2)),
        None,
    );
    let _ = a.to(extension);
}
