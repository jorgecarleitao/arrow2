use arrow2::buffer::{Buffer, MutableBuffer};

#[test]
fn default() {
    let b = MutableBuffer::<i32>::default();
    assert_eq!(b.len(), 0);
    assert!(b.is_empty());
}

#[test]
fn with_capacity() {
    let b = MutableBuffer::<i32>::with_capacity(6);
    assert!(b.capacity() >= 6);
    assert!(b.is_empty());
}

#[test]
fn from_len_zeroed() {
    let b = MutableBuffer::<i32>::from_len_zeroed(3);
    assert_eq!(b.len(), 3);
    assert!(!b.is_empty());
    assert_eq!(b.as_slice(), &[0, 0, 0]);
}

#[test]
fn resize() {
    let mut b = MutableBuffer::<i32>::new();
    b.resize(3, 1);
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[1, 1, 1]);
    assert_eq!(b.as_mut_slice(), &[1, 1, 1]);
}

// branch that uses alloc_zeroed
#[test]
fn resize_from_zero() {
    let mut b = MutableBuffer::<i32>::new();
    b.resize(3, 0);
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[0, 0, 0]);
}

#[test]
fn resize_smaller() {
    let mut b = MutableBuffer::<i32>::from_len_zeroed(3);
    b.resize(2, 1);
    assert_eq!(b.len(), 2);
    assert_eq!(b.as_slice(), &[0, 0]);
}

#[test]
fn extend_from_slice() {
    let mut b = MutableBuffer::<i32>::from_len_zeroed(1);
    b.extend_from_slice(&[1, 2]);
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[0, 1, 2]);

    assert_eq!(unsafe { *b.as_ptr() }, 0);
    assert_eq!(unsafe { *b.as_mut_ptr() }, 0);
}

#[test]
fn push() {
    let mut b = MutableBuffer::<i32>::new();
    for _ in 0..17 {
        b.push(1);
    }
    assert_eq!(b.len(), 17);
}

#[test]
fn capacity() {
    let b = MutableBuffer::<f32>::with_capacity(10);
    assert_eq!(b.capacity(), 64 / std::mem::size_of::<f32>());
    let b = MutableBuffer::<f32>::with_capacity(16);
    assert_eq!(b.capacity(), 16);

    let b = MutableBuffer::<f32>::with_capacity(64);
    assert!(b.capacity() >= 64);

    let mut b = MutableBuffer::<f32>::with_capacity(16);
    b.reserve(4);
    assert_eq!(b.capacity(), 16);
    b.extend_from_slice(&[0.1; 16]);
    b.reserve(4);
    assert_eq!(b.capacity(), 32);
}

#[test]
fn extend() {
    let mut b = MutableBuffer::<i32>::new();
    b.extend(0..3);
    assert_eq!(b.as_slice(), &[0, 1, 2]);
}

#[test]
fn extend_constant() {
    let mut b = MutableBuffer::<i32>::new();
    b.extend_constant(3, 1);
    assert_eq!(b.as_slice(), &[1, 1, 1]);
}

#[test]
fn from_iter() {
    let b = (0..3).collect::<MutableBuffer<i32>>();
    assert_eq!(b.as_slice(), &[0, 1, 2]);
}

#[test]
fn from_as_ref() {
    let b = MutableBuffer::<i32>::from(&[0, 1, 2]);
    assert_eq!(b.as_slice(), &[0, 1, 2]);
}

#[test]
fn from_trusted_len_iter() {
    let b = unsafe { MutableBuffer::<i32>::from_trusted_len_iter_unchecked(0..3) };
    assert_eq!(b.as_slice(), &[0, 1, 2]);
}

#[test]
fn try_from_trusted_len_iter() {
    let iter = (0..3).map(Result::<_, String>::Ok);
    let buffer =
        unsafe { MutableBuffer::<i32>::try_from_trusted_len_iter_unchecked(iter) }.unwrap();
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn to_buffer() {
    let b = (0..3).collect::<MutableBuffer<i32>>();
    let b: Buffer<i32> = b.into();
    assert_eq!(b.as_slice(), &[0, 1, 2]);
}

#[test]
fn debug() {
    let buffer = MutableBuffer::<i32>::from(&[0, 1, 2, 3]);
    let a = format!("{:?}", buffer);
    assert_eq!(a, "[0, 1, 2, 3]")
}
