use std::io::Cursor;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Result;
use arrow2::io::parquet::write::array_to_page;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

use parquet2::{
    compression::CompressionCodec, metadata::SchemaDescriptor, schema::io_message::from_message,
    write::write_file,
};

fn write(array: &dyn Array) -> Result<()> {
    let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
        array_to_page(array),
    )))));

    let (physical, converted) = match array.data_type() {
        DataType::Int32 => ("INT32", ""),
        DataType::Int64 => ("INT64", ""),
        DataType::Float32 => ("FLOAT", ""),
        DataType::Float64 => ("DOUBLE", ""),
        DataType::Boolean => ("BOOLEAN", ""),
        DataType::Utf8 => ("BYTE_ARRAY", "(UTF8)"),
        _ => todo!(),
    };
    let schema = SchemaDescriptor::new(
        "test".to_string(),
        vec![from_message(&format!(
            "message schema {{ OPTIONAL {} col {}; }}",
            physical, converted
        ))?],
    );

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        schema,
        CompressionCodec::Uncompressed,
        row_groups,
    )
    .unwrap();
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), DataType::Int64, 0.1);
        let a = format!("write i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_boolean_array(1024 * 2usize.pow(i), 0.1, 0.5);
        let a = format!("write bool 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 0.1);
        let a = format!("write utf8 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
