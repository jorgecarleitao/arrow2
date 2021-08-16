mod immutable;
mod mutable;
mod utils;

use arrow2::{bitmap::Bitmap, buffer::MutableBuffer};

fn create_bitmap<P: AsRef<[u8]>>(bytes: P, len: usize) -> Bitmap {
    let buffer = MutableBuffer::<u8>::from(bytes.as_ref());
    Bitmap::from_u8_buffer(buffer, len)
}

#[test]
fn eq() {
    let lhs = create_bitmap([0b01101010], 8);
    let rhs = create_bitmap([0b01001110], 8);
    assert!(lhs != rhs);
}

#[test]
fn eq_len() {
    let lhs = create_bitmap([0b01101010], 6);
    let rhs = create_bitmap([0b00101010], 6);
    assert!(lhs == rhs);
    let rhs = create_bitmap([0b00001010], 6);
    assert!(lhs != rhs);
}

#[test]
fn eq_slice() {
    let lhs = create_bitmap([0b10101010], 8).slice(1, 7);
    let rhs = create_bitmap([0b10101011], 8).slice(1, 7);
    assert!(lhs == rhs);

    let lhs = create_bitmap([0b10101010], 8).slice(2, 6);
    let rhs = create_bitmap([0b10101110], 8).slice(2, 6);
    assert!(lhs != rhs);
}

#[test]
fn and() {
    let lhs = create_bitmap([0b01101010], 8);
    let rhs = create_bitmap([0b01001110], 8);
    let expected = create_bitmap([0b01001010], 8);
    assert_eq!(&lhs & &rhs, expected);
}

#[test]
fn or_large() {
    let input: &[u8] = &[
        0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
        0b01000010, 0b11111111,
    ];
    let input1: &[u8] = &[
        0b00000000, 0b00000001, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000,
        0b10000000, 0b11111111,
    ];
    let expected: &[u8] = &[
        0b00000000, 0b00000001, 0b10000010, 0b10000100, 0b10001000, 0b10010000, 0b10100000,
        0b11000010, 0b11111111,
    ];

    let lhs = create_bitmap(input, 62);
    let rhs = create_bitmap(input1, 62);
    let expected = create_bitmap(expected, 62);
    assert_eq!(&lhs | &rhs, expected);
}

#[test]
fn and_offset() {
    let lhs = create_bitmap([0b01101011], 8).slice(1, 7);
    let rhs = create_bitmap([0b01001111], 8).slice(1, 7);
    let expected = create_bitmap([0b01001010], 8).slice(1, 7);
    assert_eq!(&lhs & &rhs, expected);
}

#[test]
fn or() {
    let lhs = create_bitmap([0b01101010], 8);
    let rhs = create_bitmap([0b01001110], 8);
    let expected = create_bitmap([0b01101110], 8);
    assert_eq!(&lhs | &rhs, expected);
}

#[test]
fn not() {
    let lhs = create_bitmap([0b01101010], 6);
    let expected = create_bitmap([0b00010101], 6);
    assert_eq!(!&lhs, expected);
}
