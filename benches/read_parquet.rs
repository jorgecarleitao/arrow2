use std::{fs::File, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::error::Result;
use arrow2::io::parquet::read::page_iter_to_array;
use parquet2::read::{get_page_iterator, read_metadata};

fn read_decompressed_pages(size: usize, column: usize) -> Result<()> {
    // reads decompressed pages (i.e. CPU)
    let dir = env!("CARGO_MANIFEST_DIR");
    let path =
        PathBuf::from(dir).join(format!("fixtures/pyarrow3/v1/basic_nulls_{}.parquet", size));
    let mut file = File::open(path).unwrap();

    let metadata = read_metadata(&mut file)?;

    let row_group = 0;
    let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

    let descriptor = &iter.descriptor().clone();
    let _ = page_iter_to_array(iter, descriptor)?;
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("read u64 10000", |b| {
        b.iter(|| read_decompressed_pages(1000, 0))
    });
    c.bench_function("read u64 100000", |b| {
        b.iter(|| read_decompressed_pages(10000, 0))
    });
    c.bench_function("read utf8 10000", |b| {
        b.iter(|| read_decompressed_pages(1000, 2))
    });
    c.bench_function("read utf8 100000", |b| {
        b.iter(|| read_decompressed_pages(10000, 2))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
