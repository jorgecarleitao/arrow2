use std::io::Read;
use std::sync::Arc;
use std::{fs, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use parquet::arrow::*;
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::SliceableCursor;

fn to_buffer(size: usize, dict: bool, multi_page: bool, compressed: bool) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed = if compressed { "snappy/" } else { "" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow3/v1/{}{}{}benches_{}.parquet",
        dict, multi_page, compressed, size
    ));

    let metadata = fs::metadata(&path).expect("unable to read_arrow metadata");
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    file.read_exact(&mut buffer).expect("buffer overflow");
    buffer
}

fn read_batch(buffer: Arc<Vec<u8>>, size: usize, column: usize) {
    let file = SliceableCursor::new(buffer);

    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader
        .get_record_reader_by_columns(vec![column], size)
        .unwrap();

    for maybe_batch in record_batch_reader {
        let batch = maybe_batch.unwrap();
        assert_eq!(batch.num_rows(), size);
    }
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|i| {
        let size = 2usize.pow(i);
        let buffer = Arc::new(to_buffer(size, false, false, false));
        let a = format!("read_arrow i64 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 0)));

        let a = format!("read_arrow utf8 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 2)));

        let a = format!("read_arrow utf8 large 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 6)));

        let a = format!("read_arrow bool 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 3)));

        let buffer = Arc::new(to_buffer(size, true, false, false));
        let a = format!("read_arrow utf8 dict 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 2)));

        let buffer = Arc::new(to_buffer(size, false, false, true));
        let a = format!("read_arrow i64 snappy 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 0)));

        let buffer = Arc::new(to_buffer(size, false, true, false));
        let a = format!("read_arrow utf8 multi 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 2)));

        let buffer = Arc::new(to_buffer(size, false, true, true));
        let a = format!("read_arrow utf8 multi snappy 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 2)));

        let buffer = Arc::new(to_buffer(size, false, true, true));
        let a = format!("read_arrow i64 multi snappy 2^{}", i);
        c.bench_function(&a, |b| b.iter(|| read_batch(buffer.clone(), size, 0)));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
