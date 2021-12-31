use std::io::Cursor;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::{clone, Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

type ChunkArc = Chunk<Arc<dyn Array>>;

fn write(array: &dyn Array, encoding: Encoding) -> Result<()> {
    let schema = Schema::from(vec![Field::new("c1", array.data_type().clone(), true)]);
    let columns: ChunkArc = Chunk::new(vec![clone(array).into()]);

    let options = WriteOptions {
        write_statistics: false,
        compression: Compression::Uncompressed,
        version: Version::V1,
    };

    let row_groups = RowGroupIterator::try_new(
        vec![Ok(columns)].into_iter(),
        &schema,
        options,
        vec![encoding],
    )?;

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        row_groups,
        &schema,
        to_parquet_schema(&schema)?,
        options,
        None,
    )?;
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), 0.1);
        let a = format!("write i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_boolean_array(1024 * 2usize.pow(i), 0.1, 0.5);
        let a = format!("write bool 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 4, 0.1, 42);
        let a = format!("write utf8 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 4, 0.1, 42);
        let a = format!("write utf8 delta 2^{}", 10 + i);
        c.bench_function(&a, |b| {
            b.iter(|| write(array, Encoding::DeltaLengthByteArray).unwrap())
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
