// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use arrow2::compute::sort::{lexsort, sort, SortColumn, SortOptions};
use arrow2::util::bench_util::*;
use arrow2::{array::*, datatypes::*};

fn create_array(size: usize, with_nulls: bool) -> PrimitiveArray<f32> {
    let null_density = if with_nulls { 0.5 } else { 0.0 };
    create_primitive_array::<f32>(size, DataType::Float32, null_density)
}

fn bench_lexsort(arr_a: &dyn Array, array_b: &dyn Array) {
    let columns = vec![
        SortColumn {
            values: arr_a,
            options: None,
        },
        SortColumn {
            values: array_b,
            options: None,
        },
    ];

    lexsort(&criterion::black_box(columns)).unwrap();
}

fn bench_sort(arr_a: &dyn Array) {
    sort(criterion::black_box(arr_a), &SortOptions::default()).unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let arr_a = create_primitive_array_with_seed::<f32>(size, DataType::Float32, 0.0, 42);

        c.bench_function(&format!("sort 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sort(&arr_a))
        });

        let arr_a = create_primitive_array_with_seed::<f32>(size, DataType::Float32, 0.1, 42);

        c.bench_function(&format!("sort null 2^{} f32", log2_size), |b| {
            b.iter(|| bench_sort(&arr_a))
        });
    });

    let arr_a = create_array(2u64.pow(10) as usize, false);
    let arr_b = create_array(2u64.pow(10) as usize, false);

    c.bench_function("lexsort 2^10", |b| b.iter(|| bench_lexsort(&arr_a, &arr_b)));

    let arr_a = create_array(2u64.pow(12) as usize, false);
    let arr_b = create_array(2u64.pow(12) as usize, false);

    c.bench_function("lexsort 2^12", |b| b.iter(|| bench_lexsort(&arr_a, &arr_b)));

    let arr_a = create_array(2u64.pow(10) as usize, true);
    let arr_b = create_array(2u64.pow(10) as usize, true);

    c.bench_function("lexsort nulls 2^10", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b))
    });

    let arr_a = create_array(2u64.pow(12) as usize, true);
    let arr_b = create_array(2u64.pow(12) as usize, true);

    c.bench_function("lexsort nulls 2^12", |b| {
        b.iter(|| bench_lexsort(&arr_a, &arr_b))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
