use arrow2::array::{Array, MutableUtf8Array, Utf8Array};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::MutableBuffer;
use arrow2::datatypes::DataType;

#[test]
fn capacities() {
    let b = MutableUtf8Array::<i32>::with_capacities(1, 10);

    assert_eq!(b.values().capacity(), 64);
    assert_eq!(b.offsets().capacity(), 16); // 64 bytes
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
    let offsets = MutableBuffer::from(&[0, 4]);
    let values = MutableBuffer::from([0, 159, 146, 150]); // invalid utf8
    MutableUtf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
}

/// Safety guarantee
#[test]
#[should_panic]
fn wrong_offsets() {
    let offsets = MutableBuffer::from(&[0, 5, 4]); // invalid offsets
    let values = MutableBuffer::from(b"abbbbb");
    MutableUtf8Array::<i32>::from_data(DataType::Utf8, offsets, values, None);
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let offsets = MutableBuffer::from(&[0, 4]); // invalid offsets
    let values = MutableBuffer::from(b"abbb");
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
