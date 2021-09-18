use arrow2::{array::*, compute::aggregate::estimated_bytes_size};

#[test]
fn primitive() {
    let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
    assert_eq!(5 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
}

#[test]
fn boolean() {
    let a = BooleanArray::from_slice(&[true]);
    assert_eq!(1, estimated_bytes_size(&a));
}

#[test]
fn utf8() {
    let a = Utf8Array::<i32>::from_slice(&["aaa"]);
    assert_eq!(3 + 2 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
}
