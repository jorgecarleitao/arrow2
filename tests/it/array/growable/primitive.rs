use arrow2::array::{
    growable::{Growable, GrowablePrimitive},
    PrimitiveArray,
};

/// tests extending from a primitive array w/ offset nor nulls
#[test]
fn basics() {
    let b = PrimitiveArray::<u8>::from(vec![Some(1), Some(2), Some(3)]);
    let mut a = GrowablePrimitive::new(vec![&b], false, 3);
    a.extend(0, 0, 2);
    let result: PrimitiveArray<u8> = a.into();
    let expected = PrimitiveArray::<u8>::from(vec![Some(1), Some(2)]);
    assert_eq!(result, expected);
}

/// tests extending from a primitive array with offset w/ nulls
#[test]
fn offset() {
    let b = PrimitiveArray::<u8>::from(vec![Some(1), Some(2), Some(3)]);
    let b = b.slice(1, 2);
    let mut a = GrowablePrimitive::new(vec![&b], false, 2);
    a.extend(0, 0, 2);
    let result: PrimitiveArray<u8> = a.into();
    let expected = PrimitiveArray::<u8>::from(vec![Some(2), Some(3)]);
    assert_eq!(result, expected);
}

/// tests extending from a primitive array with offset and nulls
#[test]
fn null_offset() {
    let b = PrimitiveArray::<u8>::from(vec![Some(1), None, Some(3)]);
    let b = b.slice(1, 2);
    let mut a = GrowablePrimitive::new(vec![&b], false, 2);
    a.extend(0, 0, 2);
    let result: PrimitiveArray<u8> = a.into();
    let expected = PrimitiveArray::<u8>::from(vec![None, Some(3)]);
    assert_eq!(result, expected);
}

#[test]
fn null_offset_validity() {
    let b = PrimitiveArray::<u8>::from(&[Some(1), Some(2), Some(3)]);
    let b = b.slice(1, 2);
    let mut a = GrowablePrimitive::new(vec![&b], true, 2);
    a.extend(0, 0, 2);
    a.extend_validity(3);
    a.extend(0, 1, 1);
    let result: PrimitiveArray<u8> = a.into();
    let expected = PrimitiveArray::<u8>::from(&[Some(2), Some(3), None, None, None, Some(3)]);
    assert_eq!(result, expected);
}

#[test]
fn joining_arrays() {
    let b = PrimitiveArray::<u8>::from(&[Some(1), Some(2), Some(3)]);
    let c = PrimitiveArray::<u8>::from(&[Some(4), Some(5), Some(6)]);
    let mut a = GrowablePrimitive::new(vec![&b, &c], false, 4);
    a.extend(0, 0, 2);
    a.extend(1, 1, 2);
    let result: PrimitiveArray<u8> = a.into();

    let expected = PrimitiveArray::<u8>::from(&[Some(1), Some(2), Some(5), Some(6)]);
    assert_eq!(result, expected);
}
