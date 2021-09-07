use std::io::Read;
use std::{fs, io::Cursor, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn to_buffer(size: usize) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(dir).join(format!("fixtures/pyarrow3/v1/benches_{}.parquet", size));
    let metadata = fs::metadata(&path).expect("unable to read metadata");
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    file.read_exact(&mut buffer).expect("buffer overflow");
    buffer
}

fn read_decompressed_pages(buffer: &[u8], size: usize, column: usize) -> Result<()> {
    let file = Cursor::new(buffer);

    let reader = read::RecordReader::try_new(file, Some(vec![column]), None, None, None)?;

    for maybe_batch in reader {
        let batch = maybe_batch?;
        assert_eq!(batch.num_rows(), size);
    }
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|i| {
        let size = 2usize.pow(i);
        let buffer = to_buffer(size);
        let a = format!("read i64 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, size * 8, 0).unwrap())
        });

        let a = format!("read utf8 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, size * 8, 2).unwrap())
        });

        let a = format!("read bool 2^{}", i);
        c.bench_function(&a, |b| {
            b.iter(|| read_decompressed_pages(&buffer, size * 8, 3).unwrap())
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
