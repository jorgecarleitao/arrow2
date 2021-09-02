use arrow2::bitmap::Bitmap;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_arrow2(lhs: &Bitmap, rhs: &Bitmap) {
    let r = lhs | rhs;
    assert!(r.null_count() > 0);
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bitmap: Bitmap = (0..size).into_iter().map(|x| x % 3 == 0).collect();
        c.bench_function(&format!("bitmap aligned not 2^{}", log2_size), |b| {
            b.iter(|| {
                let r = !&bitmap;
                assert!(r.null_count() > 0);
            })
        });
        let bitmap1 = bitmap.clone().slice(1, size - 1);
        c.bench_function(&format!("bitmap not 2^{}", log2_size), |b| {
            b.iter(|| {
                let r = !&bitmap1;
                assert!(r.null_count() > 0);
            })
        });

        let bitmap1: Bitmap = (0..size).into_iter().map(|x| x % 4 == 0).collect();
        c.bench_function(&format!("bitmap aligned or 2^{}", log2_size), |b| {
            b.iter(|| bench_arrow2(&bitmap, &bitmap1))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
