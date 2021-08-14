use arrow2::bitmap::Bitmap;

#[test]
fn as_slice() {
    let b = Bitmap::from([true, true, true, true, true, true, true, true, true]);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0b11111111, 0b1]);
    assert_eq!(offset, 0);
    assert_eq!(length, 9);
}

#[test]
fn as_slice_offset() {
    let b = Bitmap::from([true, true, true, true, true, true, true, true, true]);
    let b = b.slice(8, 1);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0b1]);
    assert_eq!(offset, 0);
    assert_eq!(length, 1);
}

#[test]
fn as_slice_offset_middle() {
    let b = Bitmap::from_u8_slice(&[0, 0, 0, 0b00010101], 27);
    let b = b.slice(22, 5);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0, 0b00010101]);
    assert_eq!(offset, 6);
    assert_eq!(length, 5);
}

#[test]
fn test_debug() {
    let b = Bitmap::from([true, true, false, true, true, true, true, true, true]);
    let b = b.slice(2, 7);

    assert_eq!(format!("{:?}", b), "[0b111110__, 0b_______1]");
}
