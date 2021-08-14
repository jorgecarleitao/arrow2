use arrow2::bitmap::{Bitmap, MutableBitmap};

#[test]
fn trusted_len() {
    let data = vec![true; 65];
    let bitmap = MutableBitmap::from_trusted_len_iter(data.into_iter());
    let bitmap: Bitmap = bitmap.into();
    assert_eq!(bitmap.len(), 65);

    assert_eq!(bitmap.as_slice().0[8], 0b00000001);
}

#[test]
fn trusted_len_small() {
    let data = vec![true; 7];
    let bitmap = MutableBitmap::from_trusted_len_iter(data.into_iter());
    let bitmap: Bitmap = bitmap.into();
    assert_eq!(bitmap.len(), 7);

    assert_eq!(bitmap.as_slice().0[0], 0b01111111);
}

#[test]
fn push() {
    let mut bitmap = MutableBitmap::new();
    bitmap.push(true);
    bitmap.push(false);
    bitmap.push(false);
    for _ in 0..7 {
        bitmap.push(true)
    }
    let bitmap: Bitmap = bitmap.into();
    assert_eq!(bitmap.len(), 10);

    assert_eq!(bitmap.as_slice().0, &[0b11111001, 0b00000011]);
}

#[test]
fn push_small() {
    let mut bitmap = MutableBitmap::new();
    bitmap.push(true);
    bitmap.push(true);
    bitmap.push(false);
    let bitmap: Option<Bitmap> = bitmap.into();
    let bitmap = bitmap.unwrap();
    assert_eq!(bitmap.len(), 3);
    assert_eq!(bitmap.as_slice().0[0], 0b00000011);
}

#[test]
fn push_exact_zeros() {
    let mut bitmap = MutableBitmap::new();
    for _ in 0..8 {
        bitmap.push(false)
    }
    let bitmap: Option<Bitmap> = bitmap.into();
    let bitmap = bitmap.unwrap();
    assert_eq!(bitmap.len(), 8);
    assert_eq!(bitmap.as_slice().0.len(), 1);
}

#[test]
fn push_exact_ones() {
    let mut bitmap = MutableBitmap::new();
    for _ in 0..8 {
        bitmap.push(true)
    }
    let bitmap: Option<Bitmap> = bitmap.into();
    assert!(bitmap.is_none());
}

#[test]
fn capacity() {
    let b = MutableBitmap::with_capacity(10);
    assert_eq!(b.capacity(), 512);

    let b = MutableBitmap::with_capacity(512);
    assert_eq!(b.capacity(), 512);

    let mut b = MutableBitmap::with_capacity(512);
    b.reserve(8);
    assert_eq!(b.capacity(), 512);
}

#[test]
fn capacity_push() {
    let mut b = MutableBitmap::with_capacity(512);
    (0..512).for_each(|_| b.push(true));
    assert_eq!(b.capacity(), 512);
    b.reserve(8);
    assert_eq!(b.capacity(), 1024);
}

#[test]
fn extend() {
    let mut b = MutableBitmap::new();

    let iter = (0..512).map(|i| i % 6 == 0);
    unsafe { b.extend_from_trusted_len_iter_unchecked(iter) };
    let b: Bitmap = b.into();
    for (i, v) in b.iter().enumerate() {
        assert_eq!(i % 6 == 0, v);
    }
}

#[test]
fn extend_offset() {
    let mut b = MutableBitmap::new();
    b.push(true);

    let iter = (0..512).map(|i| i % 6 == 0);
    unsafe { b.extend_from_trusted_len_iter_unchecked(iter) };
    let b: Bitmap = b.into();
    let mut iter = b.iter().enumerate();
    assert!(iter.next().unwrap().1);
    for (i, v) in iter {
        assert_eq!((i - 1) % 6 == 0, v);
    }
}

#[test]
fn set() {
    let mut bitmap = MutableBitmap::from_len_zeroed(12);
    bitmap.set(0, true);
    assert!(bitmap.get(0));
    bitmap.set(0, false);
    assert!(!bitmap.get(0));

    bitmap.set(11, true);
    assert!(bitmap.get(11));
    bitmap.set(11, false);
    assert!(!bitmap.get(11));
    bitmap.set(11, true);

    let bitmap: Option<Bitmap> = bitmap.into();
    let bitmap = bitmap.unwrap();
    assert_eq!(bitmap.len(), 12);
    assert_eq!(bitmap.as_slice().0[0], 0b00000000);
}

#[test]
fn extend_from_bitmap() {
    let other = Bitmap::from(&[true, false, true]);
    let mut bitmap = MutableBitmap::new();

    // call is optimized to perform a memcopy
    bitmap.extend_from_bitmap(&other);

    assert_eq!(bitmap.len(), 3);
    assert_eq!(bitmap.as_slice()[0], 0b00000101);

    // this call iterates over all bits
    bitmap.extend_from_bitmap(&other);

    assert_eq!(bitmap.len(), 6);
    assert_eq!(bitmap.as_slice()[0], 0b00101101);
}

#[test]
fn debug() {
    let mut b = MutableBitmap::new();
    assert_eq!(format!("{:?}", b), "[]");
    b.push(true);
    b.push(false);
    assert_eq!(format!("{:?}", b), "[0b______01]");
    b.push(false);
    b.push(false);
    b.push(false);
    b.push(false);
    b.push(true);
    b.push(true);
    assert_eq!(format!("{:?}", b), "[0b11000001]");
    b.push(true);
    assert_eq!(format!("{:?}", b), "[0b11000001, 0b_______1]");
}
