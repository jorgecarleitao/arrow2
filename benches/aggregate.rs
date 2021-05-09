use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::util::bench_util::*;
use arrow2::{compute::aggregate::*, datatypes::DataType};

fn bench_sum(arr_a: &PrimitiveArray<f32>) {
    sum(criterion::black_box(arr_a)).unwrap();
}

fn bench_min(arr_a: &PrimitiveArray<f32>) {
    min_primitive(criterion::black_box(arr_a)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array::<f32>(size, DataType::Float32, 0.0);

        c.bench_function(&format!("sum 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sum(&arr_a))
        });
        c.bench_function(&format!("min 2^{} f32", log2_size), |b| {
            b.iter(|| bench_min(&arr_a))
        });

        let arr_a = create_primitive_array::<f32>(size, DataType::Float32, 0.1);

        c.bench_function(&format!("sum null 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sum(&arr_a))
        });

        c.bench_function(&format!("min null 2^{} f32", log2_size), |b| {
            b.iter(|| bench_min(&arr_a))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
