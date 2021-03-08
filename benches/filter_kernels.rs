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
extern crate arrow2;

use arrow2::array::*;
use arrow2::compute::filter::{build_filter, filter, Filter};
use arrow2::datatypes::DataType;
use arrow2::util::bench_util::*;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_filter(data_array: &dyn Array, filter_array: &BooleanArray) {
    criterion::black_box(filter(data_array, filter_array).unwrap());
}

fn bench_built_filter<'a>(filter: &Filter<'a>, array: &impl Array) {
    criterion::black_box(filter(array));
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let filter_array = create_boolean_array(size, 0.0, 0.5);
    let dense_filter_array = create_boolean_array(size, 0.0, 1.0 - 1.0 / 1024.0);
    let sparse_filter_array = create_boolean_array(size, 0.0, 1.0 / 1024.0);

    let filter = build_filter(&filter_array).unwrap();
    let dense_filter = build_filter(&dense_filter_array).unwrap();
    let sparse_filter = build_filter(&sparse_filter_array).unwrap();

    let data_array = create_primitive_array::<u8>(size, DataType::UInt8, 0.0);

    c.bench_function("filter u8", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter u8 high selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter u8 low selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &sparse_filter_array))
    });

    c.bench_function("filter context u8", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<u8>(size, DataType::UInt8, 0.5);
    c.bench_function("filter context u8 w NULLs", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context u8 w NULLs low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_primitive_array::<f32>(size, DataType::Float32, 0.5);
    c.bench_function("filter f32", |b| {
        b.iter(|| bench_filter(&data_array, &filter_array))
    });
    c.bench_function("filter f32 high selectivity", |b| {
        b.iter(|| bench_filter(&data_array, &dense_filter_array))
    });
    c.bench_function("filter context f32", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context f32 high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context f32 low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });

    let data_array = create_string_array::<i32>(size, 0.5);
    c.bench_function("filter context string", |b| {
        b.iter(|| bench_built_filter(&filter, &data_array))
    });
    c.bench_function("filter context string high selectivity", |b| {
        b.iter(|| bench_built_filter(&dense_filter, &data_array))
    });
    c.bench_function("filter context string low selectivity", |b| {
        b.iter(|| bench_built_filter(&sparse_filter, &data_array))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
