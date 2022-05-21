use arrow2::types::{BitChunkIter, BitChunkOnes};

#[test]
fn test_basic1() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let iter = BitChunkIter::new(a, 16);
    let r = iter.collect::<Vec<_>>();
    assert_eq!(r, (0..16).map(|x| x == 0 || x == 12).collect::<Vec<_>>(),);
}

#[test]
fn test_ones() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let mut iter = BitChunkOnes::new(a);
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(12));
}
