use std::iter::FromIterator;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::{
    array::{ListArray, PrimitiveArray},
    bitmap::Bitmap,
    buffer::{Buffer, MutableBuffer},
    datatypes::DataType,
};

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let values = Buffer::from_iter(0..size as i32);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        let mut offsets = MutableBuffer::from_iter((0..size as i32).step_by(2));
        offsets.push(size as i32);

        let validity = (0..(offsets.len() - 1))
            .map(|i| i % 4 == 0)
            .collect::<Bitmap>();

        let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
        let array = ListArray::<i32>::from_data(
            data_type,
            offsets.into(),
            Arc::new(values),
            Some(validity),
        );

        c.bench_function(&format!("list: iter_values 2^{}", log2_size), |b| {
            b.iter(|| {
                for x in array.values_iter() {
                    assert_eq!(x.len(), 2);
                }
            })
        });

        c.bench_function(&format!("list: iter 2^{}", log2_size), |b| {
            b.iter(|| {
                for x in array.iter() {
                    assert_eq!(x.unwrap().len(), 2);
                }
            })
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
