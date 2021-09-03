use arrow2::{
    array::{Array, BinaryArray},
    bitmap::Bitmap,
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
        &Some(Bitmap::from_u8_slice(&[0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = BinaryArray::<i32>::from_data(
        DataType::Binary,
        array.offsets().clone(),
        array.values().clone(),
        array.validity().clone(),
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
    assert_eq!(array.validity(), &None);
}

#[test]
fn from() {
    let array = BinaryArray::<i32>::from(&[Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

    let a = array.validity().as_ref().unwrap();
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
