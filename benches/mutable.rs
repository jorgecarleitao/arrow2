use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::bitmap::MutableBitmap;
use arrow2::util::bench_util::*;
use arrow2::{compute::aggregate::*, datatypes::DataType};
use rand::Rng;

fn create_bools(null_density: f32, size: usize) -> Vec<bool> {
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                false
            } else {
                true
            }
        })
        .collect()
}

fn bench_build(values: &[bool]) {
    criterion::black_box({
        let mut bitmap = MutableBitmap::new();

        values.iter().for_each(|v| bitmap.push(*v));
    })
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let null_density = 0.2;
        let values = create_bools(null_density, size);

        c.bench_function(
            &format!(
                "mutable bitmap 2^{} null density: {}",
                log2_size, null_density
            ),
            |b| b.iter(|| bench_build(&values)),
        );
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
