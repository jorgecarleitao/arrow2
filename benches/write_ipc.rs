use std::io::Cursor;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow2::array::*;
use arrow2::columns::Columns;
use arrow2::datatypes::{Field, Schema};
use arrow2::error::Result;
use arrow2::io::ipc::write::*;
use arrow2::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

fn write(array: &dyn Array) -> Result<()> {
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);
    let columns = Columns::try_new(vec![clone(array).into()])?;

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::try_new(writer, &schema, None, Default::default())?;

    writer.write(&columns, None)
}

fn add_benchmark(c: &mut Criterion) {
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), 0.1);
        let a = format!("write i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_boolean_array(1024 * 2usize.pow(i), 0.1, 0.5);
        let a = format!("write bool 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });

    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 4, 0.1, 42);
        let a = format!("write utf8 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
