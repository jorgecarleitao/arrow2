use std::ops::AddAssign;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::util::bench_util::*;
use arrow2::{compute::aggregate::sum, datatypes::DataType, types::NativeType};

fn bench_op<T>(arr_a: &PrimitiveArray<T>)
where
    T: NativeType + std::iter::Sum + AddAssign,
{
    sum(criterion::black_box(arr_a)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let arr_a = create_primitive_array::<f32>(size, DataType::Float32, 0.0);

    c.bench_function("add f32", |b| b.iter(|| bench_op(&arr_a)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
