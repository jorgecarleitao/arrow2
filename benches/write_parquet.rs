use std::io::Cursor;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::record_batch::RecordBatch;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

fn write(array: &dyn Array, encoding: Encoding) -> Result<()> {
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);

    let options = WriteOptions {
        write_statistics: false,
        compression: Compression::Uncompressed,
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

fn prepare(schema: &Schema) -> Result<(WriteOptions, Encoding, SchemaDescriptor)> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let encoding = Encoding::Plain;

    // map arrow fields to parquet fields
    let parquet_schema = to_parquet_schema(&schema)?;
    Ok((options, encoding, parquet_schema))
}

fn write_parallel(array: &dyn Array, encoding: Encoding) -> Result<()> {
    let array: Arc<dyn Array> = clone(array).into();
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field.clone(), field.clone()]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array.clone(), array])?;

    // prepare a channel to send serialized records from threads
    let (tx, rx) = mpsc::sync_channel(2);
    let mut children = Vec::new();

    let (options, encoding, parquet_schema) = prepare(batch.schema())?;

    batch
        .columns()
        .iter()
        .zip(parquet_schema.columns().to_vec().into_iter())
        .for_each(|(array, descriptor)| {
            let array = array.clone(); // note: this is cheap

            // The sender endpoint can be cloned
            let thread_tx = tx.clone();

            let options = options.clone();
            let child = thread::spawn(move || {
                // create compressed pages
                let pages = array_to_pages(array, descriptor, options, encoding)
                    .unwrap()
                    .collect::<Vec<_>>();
                thread_tx.send(pages).unwrap();
            });

            children.push(child);
        });

    let row_groups = std::iter::once(Result::Ok(DynIter::new(
        (0..batch.num_columns()).map(|_| Ok(DynIter::new(rx.recv().unwrap().into_iter()))),
    )));

    let mut writer = Cursor::new(vec![]);

    // Write the file. Note that, at present, any error results in a corrupted file.
    let _ = write_file(
        &mut writer,
        row_groups,
        batch.schema(),
        parquet_schema,
        options,
        None,
    )?;

    for child in children {
        child.join().expect("child thread panicked");
    }
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

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), DataType::Int64, 0.1);
        let a = format!("write parallel i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array, Encoding::Plain).unwrap()));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
