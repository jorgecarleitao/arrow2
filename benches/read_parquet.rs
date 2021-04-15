use std::io::Read;
use std::{fs, io::Cursor, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::error::Result;
use arrow2::io::parquet::read::page_iter_to_array;
use parquet2::read::{get_page_iterator, read_metadata};

fn to_buffer(size: usize) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(dir).join(format!("fixtures/pyarrow3/v1/benches_{}.parquet", size));
    let metadata = fs::metadata(&path).expect("unable to read metadata");
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    file.read(&mut buffer).expect("buffer overflow");
    buffer
}

fn read_decompressed_pages(buffer: &[u8], column: usize) -> Result<()> {
    let mut file = Cursor::new(buffer);

    let metadata = read_metadata(&mut file)?;

    let row_group = 0;
    let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

    let descriptor = &iter.descriptor().clone();
    let _ = page_iter_to_array(iter, descriptor)?;
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|i| {
        let buffer = to_buffer(2usize.pow(i));
        let a = format!("read i64 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, 0).unwrap())
        });
    });

    (10..=20).step_by(2).for_each(|i| {
        let buffer = to_buffer(2usize.pow(i));
        let a = format!("read utf8 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, 2).unwrap())
        });
    });

    (10..=20).step_by(2).for_each(|i| {
        let buffer = to_buffer(2usize.pow(i));
        let a = format!("read bool 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, 3).unwrap())
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
