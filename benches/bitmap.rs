extern crate arrow2;

use std::iter::FromIterator;

use arrow2::bitmap::*;

use criterion::{criterion_group, criterion_main, Criterion};

//

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bitmap2 = Bitmap::from_iter((0..size).into_iter().map(|x| x % 3 == 0));

        c.bench_function(&format!("bitmap extend aligned 2^{}", log2_size), |b| {
            let mut bitmap1 = MutableBitmap::new();
            b.iter(|| {
                bitmap1.extend_from_bitmap(&bitmap2);
                bitmap1.clear();
            })
        });

        c.bench_function(&format!("bitmap extend unaligned 2^{}", log2_size), |b| {
            let mut bitmap1 = MutableBitmap::with_capacity(1);
            b.iter(|| {
                bitmap1.push(true);
                bitmap1.extend_from_bitmap(&bitmap2);
                bitmap1.clear();
            })
        });

        c.bench_function(
            &format!("bitmap extend_constant aligned 2^{}", log2_size),
            |b| {
                let mut bitmap1 = MutableBitmap::new();
                b.iter(|| {
                    bitmap1.extend_constant(size, true);
                    bitmap1.clear();
                })
            },
        );

        c.bench_function(
            &format!("bitmap extend_constant unaligned 2^{}", log2_size),
            |b| {
                let mut bitmap1 = MutableBitmap::with_capacity(1);
                b.iter(|| {
                    bitmap1.push(true);
                    bitmap1.extend_constant(size, true);
                    bitmap1.clear();
                })
            },
        );
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
