use rand::distributions::{Bernoulli, Uniform};
use rand::prelude::StdRng;
use rand::Rng;
use rand::SeedableRng;

use arrow2::bitmap::utils::SlicesIterator;
use arrow2::bitmap::Bitmap;

#[test]
fn check_invariant() {
    let values = (0..8).map(|i| i % 2 != 0).collect::<Bitmap>();
    let iter = SlicesIterator::new(&values);

    let slots = iter.slots();

    let slices = iter.collect::<Vec<_>>();

    assert_eq!(slices, vec![(1, 1), (3, 1), (5, 1), (7, 1)]);

    let mut sum = 0;
    for (_, len) in slices {
        sum += len;
    }
    assert_eq!(sum, slots);
}

#[test]
fn single_set() {
    let values = (0..16).map(|i| i == 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn single_unset() {
    let values = (0..64).map(|i| i != 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(0, 1), (2, 62)]);
    assert_eq!(count, 64 - 1);
}

#[test]
fn generic() {
    let values = (0..130).map(|i| i % 62 != 0).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 61), (63, 61), (125, 5)]);
    assert_eq!(count, 61 + 61 + 5);
}

#[test]
fn incomplete_byte() {
    let values = (0..6).map(|i| i == 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn incomplete_byte1() {
    let values = (0..12).map(|i| i == 9).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(9, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn end_of_byte() {
    let values = (0..16).map(|i| i != 7).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(0, 7), (8, 8)]);
    assert_eq!(count, 15);
}

#[test]
fn bla() {
    let values = vec![true, true, true, true, true, true, true, false]
        .into_iter()
        .collect::<Bitmap>();
    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    assert_eq!(values.null_count() + iter.slots(), values.len());

    let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

    assert_eq!(count, total);
}

#[test]
fn past_end_should_not_be_returned() {
    let values = Bitmap::from_u8_slice(&[0b11111010], 3);
    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    assert_eq!(values.null_count() + iter.slots(), values.len());

    let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

    assert_eq!(count, total);
}

#[test]
fn sliced() {
    let values = Bitmap::from_u8_slice(&[0b11111010, 0b11111011], 16);
    let values = values.slice(8, 2);
    let iter = SlicesIterator::new(&values);

    let chunks = iter.collect::<Vec<_>>();

    // the first "11" in the second byte
    assert_eq!(chunks, vec![(0, 2)]);
}

#[test]
fn remainder_1() {
    let values = Bitmap::from_u8_slice(&[0, 0, 0b00000000, 0b00010101], 27);
    let values = values.slice(22, 5);
    let iter = SlicesIterator::new(&values);
    let chunks = iter.collect::<Vec<_>>();
    assert_eq!(chunks, vec![(2, 1), (4, 1)]);
}

#[test]
fn filter_slices() {
    let mut rng = StdRng::seed_from_u64(42);
    let length = 500;

    let mask: Bitmap = (0..length)
        .map(|_| {
            let v: bool = (&mut rng).sample(Bernoulli::new(0.5).unwrap());
            v
        })
        .collect();

    for offset in 100usize..(length - 1) {
        let len = (&mut rng).sample(Uniform::new(0, length - offset));
        let mask_s = mask.clone().slice(offset, len);

        let iter = SlicesIterator::new(&mask_s);
        iter.for_each(|(start, slice_len)| {
            if start + slice_len > len {
                panic!("Fail")
            }
        });
    }
}
