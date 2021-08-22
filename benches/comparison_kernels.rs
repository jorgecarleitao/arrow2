use criterion::{black_box, criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::scalar::*;
use arrow2::util::bench_util::*;
use arrow2::{compute::comparison::*, datatypes::DataType};

fn bench_op(arr_a: &dyn Array, arr_b: &dyn Array, op: Operator) {
    compare(black_box(arr_a), black_box(arr_b), op).unwrap();
}

fn bench_op_scalar(arr_a: &dyn Array, value_b: &dyn Scalar, op: Operator) {
    compare_scalar(black_box(arr_a), black_box(value_b), op).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let arr_a = create_primitive_array_with_seed::<f32>(size, DataType::Float32, 0.0, 42);
        let arr_b = create_primitive_array_with_seed::<f32>(size, DataType::Float32, 0.0, 43);

        c.bench_function(&format!("f32 2^{}", log2_size), |b| {
            b.iter(|| bench_op(&arr_a, &arr_b, Operator::Eq))
        });
        c.bench_function(&format!("f32 scalar 2^{}", log2_size), |b| {
            b.iter(|| {
                bench_op_scalar(
                    &arr_a,
                    &PrimitiveScalar::<f32>::from(Some(0.5)),
                    Operator::Eq,
                )
            })
        });

        let arr_a = create_boolean_array(size, 0.0, 0.1);
        let arr_b = create_boolean_array(size, 0.0, 0.2);

        c.bench_function(&format!("bool 2^{}", log2_size), |b| {
            b.iter(|| bench_op(&arr_a, &arr_b, Operator::Eq))
        });
        c.bench_function(&format!("bool scalar 2^{}", log2_size), |b| {
            b.iter(|| bench_op_scalar(&arr_a, &BooleanScalar::from(Some(true)), Operator::Eq))
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
