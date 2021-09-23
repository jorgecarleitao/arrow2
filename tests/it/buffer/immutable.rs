use arrow2::buffer::Buffer;

#[test]
fn new() {
    let buffer = Buffer::<i32>::new();
    assert_eq!(buffer.len(), 0);
    assert!(buffer.is_empty());
}

#[test]
fn new_zeroed() {
    let buffer = Buffer::<i32>::new_zeroed(2);
    assert_eq!(buffer.len(), 2);
    assert!(!buffer.is_empty());
    assert_eq!(buffer.as_slice(), &[0, 0]);
}

#[test]
fn from_slice() {
    let buffer = Buffer::<i32>::from(&[0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn slice() {
    let buffer = Buffer::<i32>::from(&[0, 1, 2, 3]);
    let buffer = buffer.slice(1, 2);
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.as_slice(), &[1, 2]);
}

#[test]
fn from_iter() {
    let buffer = (0..3).collect::<Buffer<i32>>();
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn from_trusted_len_iter() {
    let buffer = unsafe { Buffer::<i32>::from_trusted_len_iter_unchecked(0..3) };
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn try_from_trusted_len_iter() {
    let iter = (0..3).map(Result::<_, String>::Ok);
    let buffer = unsafe { Buffer::<i32>::try_from_trusted_len_iter_unchecked(iter) }.unwrap();
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn as_ptr() {
    let buffer = Buffer::<i32>::from(&[0, 1, 2, 3]);
    let buffer = buffer.slice(1, 2);
    let ptr = buffer.as_ptr();
    assert_eq!(unsafe { *ptr }, 1);
}

#[test]
fn debug() {
    let buffer = Buffer::<i32>::from(&[0, 1, 2, 3]);
    let buffer = buffer.slice(1, 2);
    let a = format!("{:?}", buffer);
    assert_eq!(a, "[1, 2]")
}

#[cfg(not(feature = "cache_aligned"))]
#[test]
fn from_vec() {
    let buffer = Buffer::<i32>::from_vec(vec![0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}
