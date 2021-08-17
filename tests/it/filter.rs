use arrow2::array::{Array, BooleanArray, Utf8Array};
use arrow2::bitmap::utils::SlicesIterator;
use rand::distributions::{Alphanumeric, Bernoulli, Uniform};
use rand::prelude::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::iter::FromIterator;

#[test]
fn filter_slices() {
    let mut rng = StdRng::seed_from_u64(42);
    let length = 50000;

    let values_iter = (0..length).map(|_| {
        let len = (&mut rng).sample(Uniform::new(0usize, 8));
        let s: String = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect();
        s
    });

    let arr = Utf8Array::<i32>::from_iter_values(values_iter);
    let values_iter = (0..length).map(|_| {
        let v: bool = (&mut rng).sample(Bernoulli::new(0.5).unwrap());
        Some(v)
    });
    let mask = BooleanArray::from_iter(values_iter);

    for offset in 100usize..(length - 1) {
        let len = (&mut rng).sample(Uniform::new(0, length - offset));
        let arr_s = arr.slice(offset, len);
        let mask_s = mask.slice(offset, len);

        let iter = SlicesIterator::new(mask_s.values());
        iter.for_each(|(start, len)| {
            if start + len > arr_s.len() {
                panic!("Fail")
            }
        });
    }
}
