use criterion::{criterion_group, criterion_main, Criterion};

fn dummy() {}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("dummy", |b| b.iter(|| dummy()));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
