use arrow2::bitmap::{binary_assign, unary_assign, Bitmap, MutableBitmap};

#[test]
fn basics() {
    let mut b = MutableBitmap::from_iter(std::iter::repeat(true).take(10));
    unary_assign(&mut b, |x: u8| !x);
    assert_eq!(
        b,
        MutableBitmap::from_iter(std::iter::repeat(false).take(10))
    );

    let mut b = MutableBitmap::from_iter(std::iter::repeat(true).take(10));
    let c = Bitmap::from_iter(std::iter::repeat(true).take(10));
    binary_assign(&mut b, &c, |x: u8, y| x | y);
    assert_eq!(
        b,
        MutableBitmap::from_iter(std::iter::repeat(true).take(10))
    );
}

#[test]
fn fast_paths() {
    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([true, true]);
    let b = b & &c;
    assert_eq!(b, MutableBitmap::from_iter([true, false]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([false, false]);
    let b = b & &c;
    assert_eq!(b, MutableBitmap::from_iter([false, false]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([true, true]);
    let b = b | &c;
    assert_eq!(b, MutableBitmap::from_iter([true, true]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([false, false]);
    let b = b | &c;
    assert_eq!(b, MutableBitmap::from_iter([true, false]));
}
