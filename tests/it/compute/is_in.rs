use arrow2::array::*;
use arrow2::compute::is_in::is_in;

#[test]
fn test_is_in_primitive() {
    let values = Int32Array::from([Some(1), Some(2), Some(3), None]);
    let list = Int32Array::from([Some(1), None, Some(3)]);
    let expected = BooleanArray::from([Some(true), Some(false), Some(true), Some(true)]);

    let values = is_in(&values, &list).unwrap();

    assert_eq!(expected, values);
}

#[test]
fn test_is_in_binary() {
    let values = BinaryArray::<i32>::from([Some(b"a"), Some(b"b"), Some(b"c"), None]);
    let list = BinaryArray::<i32>::from([Some(b"a"), Some(b"c")]);
    let expected = BooleanArray::from([Some(true), Some(false), Some(true), Some(false)]);

    let values = is_in(&values, &list).unwrap();

    assert_eq!(expected, values);
}

#[test]
fn test_is_in_utf8() {
    let values = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c"), None]);
    let list = Utf8Array::<i32>::from([Some("b"), None, Some("c")]);
    let expected = BooleanArray::from([Some(false), Some(true), Some(true), Some(true)]);

    let values = is_in(&values, &list).unwrap();

    assert_eq!(expected, values);
}
