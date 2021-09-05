use std::sync::Arc;

use arrow2::util::bench_util::*;
use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::csv::write;
use arrow2::record_batch::RecordBatch;

fn write_batch(batch: &RecordBatch) -> Result<()> {
    let writer = &mut write::WriterBuilder::new().from_writer(vec![]);

    write::write_header(writer, batch.schema())?;

    let options = write::SerializeOptions::default();
    write::write_batch(writer, batch, &options)
}

fn make_batch(array: impl Array + 'static) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        array.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn add_benchmark(c: &mut Criterion) {
    (10..=18).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let array = create_primitive_array::<i32>(size, DataType::Int32, 0.1);
        let batch = make_batch(array);

        c.bench_function(&format!("csv write i32 2^{}", log2_size), |b| {
            b.iter(|| write_batch(&batch))
        });

        let array = create_string_array::<i32>(size, 100, 0.1, 42);
        let batch = make_batch(array);

        c.bench_function(&format!("csv write utf8 2^{}", log2_size), |b| {
            b.iter(|| write_batch(&batch))
        });

        let array = create_primitive_array::<f64>(size, DataType::Float64, 0.1);
        let batch = make_batch(array);

        c.bench_function(&format!("csv write f64 2^{}", log2_size), |b| {
            b.iter(|| write_batch(&batch))
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
