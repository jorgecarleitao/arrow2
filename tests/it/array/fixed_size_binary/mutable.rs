use arrow2::array::*;
use arrow2::bitmap::Bitmap;

#[test]
fn basic() {
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
    assert_eq!(array.validity(), &Some(Bitmap::from([false])));
}
