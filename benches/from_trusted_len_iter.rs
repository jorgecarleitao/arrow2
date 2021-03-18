extern crate arrow2;

use arrow2::{array::Primitive, bitmap::*, buffer::*};

use criterion::{criterion_group, criterion_main, Criterion};

fn add_benchmark(c: &mut Criterion) {
    let values = 0..1026;

    let values = values.collect::<Vec<_>>();
    c.bench_function("buffer", |b| {
        b.iter(|| unsafe { MutableBuffer::from_trusted_len_iter(values.clone().into_iter()) })
    });

    let bools = values.clone().into_iter().map(|x| x % 5 == 0);
    c.bench_function("bitmap", |b| {
        b.iter(|| unsafe { MutableBitmap::from_trusted_len_iter(bools.clone()) })
    });

    let maybe_values = values
        .into_iter()
        .map(|x| if x % 5 == 0 { Some(x) } else { None });
    c.bench_function("primitive", |b| {
        b.iter(|| unsafe { Primitive::from_trusted_len_iter(maybe_values.clone()) })
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
