use arrow2::buffer::Buffer;

#[test]
fn new() {
    let buffer = Buffer::<i32>::new();
    assert_eq!(buffer.len(), 0);
    assert!(buffer.is_empty());
}

#[test]
fn from_slice() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn slice() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2, 3]);
    let buffer = buffer.sliced(1, 2);
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
fn debug() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2, 3]);
    let buffer = buffer.sliced(1, 2);
    let a = format!("{buffer:?}");
    assert_eq!(a, "[1, 2]")
}

#[test]
fn from_vec() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
#[cfg(feature = "arrow")]
fn from_arrow() {
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[1, 2, 3]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = buffer.slice(4);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 2);
    assert_eq!(b.as_slice(), &[2, 3]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i64, 2_i64]);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 4);
    assert_eq!(b.as_slice(), &[1, 0, 2, 0]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = buffer.slice(4);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[0, 2, 0]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);
}

#[test]
#[cfg(feature = "arrow")]
#[should_panic(expected = "not aligned")]
fn from_arrow_misaligned() {
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]).slice(1);
    let _ = Buffer::<i32>::from(buffer.clone());
}
