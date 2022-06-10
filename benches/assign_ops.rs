use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::{compute::arithmetics::basic::mul_scalar, util::bench_util::create_primitive_array};

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let mut arr_a = create_primitive_array::<f32>(size, 0.2);
        c.bench_function(&format!("apply_mul 2^{}", log2_size), |b| {
            b.iter(|| {
                criterion::black_box(&mut arr_a)
                    .apply_values(|x| x.iter_mut().for_each(|x| *x *= 1.01));
                assert!(!arr_a.value(10).is_nan());
            })
        });

        let arr_a = create_primitive_array::<f32>(size, 0.2);
        c.bench_function(&format!("mul 2^{}", log2_size), |b| {
            b.iter(|| {
                let a = mul_scalar(criterion::black_box(&arr_a), &1.01f32);
                assert!(!a.value(10).is_nan());
            })
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
