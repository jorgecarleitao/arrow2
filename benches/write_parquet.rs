use std::io::Cursor;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

fn write(array: &dyn Array, encoding: Encoding) -> Result<()> {
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionCodec::Uncompressed,
        version: Version::V1,
    };

    let parquet_schema = to_parquet_schema(&schema)?;

    let row_groups = std::iter::once(Result::Ok(DynIter::new(std::iter::once(Ok(DynIter::new(
        std::iter::once(array_to_page(
            array,
            parquet_schema.columns()[0].clone(),
            options,
            encoding,
        )),
    ))))));

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), DataType::Int64, 0.1);
        let a = format!("write i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_boolean_array(1024 * 2usize.pow(i), 0.1, 0.5);
        let a = format!("write bool 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 0.1);
        let a = format!("write utf8 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 0.1);
        let a = format!("write utf8 delta 2^{}", 10 + i);
        c.bench_function(&a, |b| {
            b.iter(|| write(array, Encoding::DeltaLengthByteArray).unwrap())
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
