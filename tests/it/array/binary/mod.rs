use arrow2::{
    array::{Array, BinaryArray},
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::DataType,
};

mod mutable;

#[test]
fn basics() {
    let data = vec![Some(b"hello".to_vec()), None, Some(b"hello2".to_vec())];

    let array: BinaryArray<i32> = data.into_iter().collect();

    assert_eq!(array.value(0), b"hello");
    assert_eq!(array.value(1), b"");
    assert_eq!(array.value(2), b"hello2");
    assert_eq!(unsafe { array.value_unchecked(2) }, b"hello2");
    assert_eq!(array.values().as_slice(), b"hellohello2");
    assert_eq!(array.offsets().as_slice(), &[0, 5, 5, 11]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = BinaryArray::<i32>::from_data(
        DataType::Binary,
        array.offsets().clone(),
        array.values().clone(),
        array.validity().cloned(),
    );
    assert_eq!(array, array2);

    let array = array.slice(1, 2);
    assert_eq!(array.value(0), b"");
    assert_eq!(array.value(1), b"hello2");
    // note how this keeps everything: the offsets were sliced
    assert_eq!(array.values().as_slice(), b"hellohello2");
    assert_eq!(array.offsets().as_slice(), &[5, 5, 11]);
}

#[test]
fn empty() {
    let array = BinaryArray::<i32>::new_empty(DataType::Binary);
    assert_eq!(array.values().as_slice(), b"");
    assert_eq!(array.offsets().as_slice(), &[0]);
    assert_eq!(array.validity(), None);
}

#[test]
fn from() {
    let array = BinaryArray::<i32>::from(&[Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

    let a = array.validity().unwrap();
    assert_eq!(a, &Bitmap::from([true, true, false]));
}

#[test]
fn from_trusted_len_iter() {
    let iter = std::iter::repeat(b"hello").take(2).map(Some);
    let a = BinaryArray::<i32>::from_trusted_len_iter(iter);
    assert_eq!(a.len(), 2);
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(b"hello").take(2).map(Some);
    let a: BinaryArray<i32> = iter.collect();
    assert_eq!(a.len(), 2);
}

#[test]
fn with_validity() {
    let array = BinaryArray::<i32>::from(&[Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

    let array = array.with_validity(None);

    let a = array.validity();
    assert_eq!(a, None);
}

#[test]
#[should_panic]
fn wrong_offsets() {
    let offsets = Buffer::from(vec![0, 5, 4]); // invalid offsets
    let values = Buffer::from(b"abbbbb".to_vec());
    BinaryArray::<i32>::from_data(DataType::Binary, offsets, values, None);
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let offsets = Buffer::from(vec![0, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    BinaryArray::<i32>::from_data(DataType::Int8, offsets, values, None);
}

#[test]
#[should_panic]
fn value_with_wrong_offsets_panics() {
    let offsets = Buffer::from(vec![0, 10, 11, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    // the 10-11 is not checked
    let array = BinaryArray::<i32>::from_data(DataType::Binary, offsets, values, None);

    // but access is still checked (and panics)
    // without checks, this would result in reading beyond bounds
    array.value(0);
}

#[test]
#[should_panic]
fn index_out_of_bounds_panics() {
    let offsets = Buffer::from(vec![0, 1, 2, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    let array = BinaryArray::<i32>::from_data(DataType::Utf8, offsets, values, None);

    array.value(3);
}

#[test]
#[should_panic]
fn value_unchecked_with_wrong_offsets_panics() {
    let offsets = Buffer::from(vec![0, 10, 11, 4]);
    let values = Buffer::from(b"abbb".to_vec());
    // the 10-11 is not checked
    let array = BinaryArray::<i32>::from_data(DataType::Binary, offsets, values, None);

    // but access is still checked (and panics)
    // without checks, this would result in reading beyond bounds,
    // even if `0` is in bounds
    unsafe { array.value_unchecked(0) };
}

#[test]
fn debug() {
    let array = BinaryArray::<i32>::from(&[Some([1, 2].as_ref()), Some(&[]), None]);

    assert_eq!(format!("{:?}", array), "BinaryArray[[1, 2], [], None]");
}
