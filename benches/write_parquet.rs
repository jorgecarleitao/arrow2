use std::io::Cursor;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Result;
use arrow2::io::parquet::write::array_to_page;
use arrow2::util::bench_util::create_primitive_array;

use parquet2::{
    compression::CompressionCodec, metadata::SchemaDescriptor, schema::io_message::from_message,
    write::write_file,
};

fn write(array: &dyn Array) -> Result<()> {
    let row_groups = std::iter::once(Result::Ok(std::iter::once(Ok(std::iter::once(
        array_to_page(array),
    )))));

    // prepare schema
    let a = match array.data_type() {
        DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        _ => todo!(),
    };
    let schema = SchemaDescriptor::new(from_message(&format!(
        "message schema {{ OPTIONAL {} col; }}",
        a
    ))?);

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        &schema,
        CompressionCodec::Uncompressed,
        row_groups,
    )
    .unwrap();
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    // parity with `parquet` crates' bench
    let array = &create_primitive_array::<i64>(1024, DataType::Int64, 0.1);
    c.bench_function("write i64 2^10", |b| b.iter(|| write(array).unwrap()));

    let array = &create_primitive_array::<i64>(1024 * 4, DataType::Int64, 0.1);
    c.bench_function("write i64 2^12", |b| b.iter(|| write(array).unwrap()));
    let array = &create_primitive_array::<i64>(1024 * 4 * 4, DataType::Int64, 0.1);
    c.bench_function("write i64 2^14", |b| b.iter(|| write(array).unwrap()));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
