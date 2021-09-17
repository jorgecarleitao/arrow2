use arrow2::array::{Array, BinaryArray, MutableBinaryArray};
use arrow2::bitmap::Bitmap;

#[test]
fn push_null() {
    let mut array = MutableBinaryArray::<i32>::new();
    array.push::<&str>(None);

    let array: BinaryArray<i32> = array.into();
    assert_eq!(array.validity(), &Some(Bitmap::from([false])));
}
