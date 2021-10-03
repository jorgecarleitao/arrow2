use arrow2::array::{BinaryArray, MutableBinaryArray};
use arrow2::bitmap::Bitmap;

#[test]
fn push_null() {
    let mut array = MutableBinaryArray::<i32>::new();
    array.push::<&str>(None);

    let array: BinaryArray<i32> = array.into();
    assert_eq!(array.validity(), Some(&Bitmap::from([false])));
}

#[test]
fn extend_trusted_len_values() {
    let mut array = MutableBinaryArray::<i32>::new();

    array.extend_trusted_len_values(vec![b"first".to_vec(), b"second".to_vec()].into_iter());
    array.extend_trusted_len_values(vec![b"third".to_vec()].into_iter());
    array.extend_trusted_len(vec![None, Some(b"fourth".to_vec())].into_iter());

    let array: BinaryArray<i32> = array.into();

    assert_eq!(array.values().as_slice(), b"firstsecondthirdfourth");
    assert_eq!(array.offsets().as_slice(), &[0, 5, 11, 16, 16, 22]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00010111], 5))
    );
}

#[test]
fn extend_trusted_len() {
    let mut array = MutableBinaryArray::<i32>::new();

    array.extend_trusted_len(vec![Some(b"first".to_vec()), Some(b"second".to_vec())].into_iter());
    array.extend_trusted_len(vec![None, Some(b"third".to_vec())].into_iter());

    let array: BinaryArray<i32> = array.into();

    assert_eq!(array.values().as_slice(), b"firstsecondthird");
    assert_eq!(array.offsets().as_slice(), &[0, 5, 11, 11, 16]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00001011], 4))
    );
}
