#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow2::array::*;
use arrow2::util::bench_util::*;
use arrow2::{
    compute::arithmetics::basic::add::add, compute::arithmetics::basic::div::div_scalar,
    datatypes::DataType, types::NativeType,
};
use num_traits::NumCast;
use std::ops::{Add, Div};

fn bench_div_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T)
where
    T: NativeType + Div<Output = T> + NumCast,
{
    criterion::black_box(div_scalar(lhs, rhs));
}

fn bench_add<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>)
where
    T: NativeType + Add<Output = T> + NumCast,
{
    criterion::black_box(add(lhs, rhs)).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array_with_seed::<u64>(size, DataType::UInt64, 0.0, 43);
        let arr_b = create_primitive_array_with_seed::<u64>(size, DataType::UInt64, 0.0, 42);

        c.bench_function(&format!("divide_scalar 2^{}", log2_size), |b| {
            // 4 is a very fast optimizable divisor
            b.iter(|| bench_div_scalar(&arr_a, &4))
        });
        c.bench_function(&format!("divide_scalar prime 2^{}", log2_size), |b| {
            // large prime number that is probably harder to simplify
            b.iter(|| bench_div_scalar(&arr_a, &524287))
        });

        c.bench_function(&format!("add 2^{}", log2_size), |b| {
            b.iter(|| bench_add(&arr_a, &arr_b))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
