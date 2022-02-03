use arrow2::array::{MutableUtf8Array, Utf8Array};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;

#[test]
fn capacities() {
    let b = MutableUtf8Array::<i32>::with_capacities(1, 10);

    assert!(b.values().capacity() >= 10);
    assert!(b.offsets().capacity() >= 2);
}

#[test]
fn push_null() {
    let mut array = MutableUtf8Array::<i32>::new();
    array.push::<&str>(None);

    let array: Utf8Array<i32> = array.into();
    assert_eq!(array.validity(), Some(&Bitmap::from([false])));
}

/// Safety guarantee
#[test]
#[should_panic]
fn not_utf8() {
    let offsets = vec![0, 4];
    let values = vec![0, 159, 146, 150]; // invalid utf8
    MutableUtf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
}

/// Safety guarantee
#[test]
#[should_panic]
fn wrong_offsets() {
    let offsets = vec![0, 5, 4]; // invalid offsets
    let values = vec![0, 1, 2, 3, 4, 5];
    MutableUtf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let offsets = vec![0, 4]; // invalid offsets
    let values = vec![1, 2, 3, 4];
    MutableUtf8Array::<i32>::from_data(DataType::Int8, offsets, values, None);
}

#[test]
fn test_extend_trusted_len_values() {
    let mut array = MutableUtf8Array::<i32>::new();

    array.extend_trusted_len_values(["hi", "there"].iter());
    array.extend_trusted_len_values(["hello"].iter());
    array.extend_trusted_len(vec![Some("again"), None].into_iter());

    let array: Utf8Array<i32> = array.into();

    assert_eq!(array.values().as_slice(), b"hitherehelloagain");
    assert_eq!(array.offsets().as_slice(), &[0, 2, 7, 12, 17, 17]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00001111], 5))
    );
}

#[test]
fn test_extend_trusted_len() {
    let mut array = MutableUtf8Array::<i32>::new();

    array.extend_trusted_len(vec![Some("hi"), Some("there")].into_iter());
    array.extend_trusted_len(vec![None, Some("hello")].into_iter());
    array.extend_trusted_len_values(["again"].iter());

    let array: Utf8Array<i32> = array.into();

    assert_eq!(array.values().as_slice(), b"hitherehelloagain");
    assert_eq!(array.offsets().as_slice(), &[0, 2, 7, 7, 12, 17]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00011011], 5))
    );
}

#[test]
fn test_extend_values() {
    let mut array = MutableUtf8Array::<i32>::new();

    array.extend_values([Some("hi"), None, Some("there"), None].iter().flatten());
    array.extend_values([Some("hello"), None].iter().flatten());
    array.extend_values(vec![Some("again"), None].into_iter().flatten());

    let array: Utf8Array<i32> = array.into();

    assert_eq!(array.values().as_slice(), b"hitherehelloagain");
    assert_eq!(array.offsets().as_slice(), &[0, 2, 7, 12, 17]);
    assert_eq!(array.validity(), None,);
}
