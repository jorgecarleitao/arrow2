use arrow2::array::{Array, MutableUtf8Array, Utf8Array};
use arrow2::bitmap::Bitmap;

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
    assert_eq!(array.validity(), &Some(Bitmap::from([false])));
}
