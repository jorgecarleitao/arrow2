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

use arrow2::array::*;
use arrow2::util::bench_util::*;
use arrow2::{compute::comparison::*, datatypes::DataType, types::NativeType};

fn bench_op<T>(arr_a: &PrimitiveArray<T>, arr_b: &PrimitiveArray<T>, op: Operator)
where
    T: NativeType,
{
    compare(criterion::black_box(arr_a), criterion::black_box(arr_b), op).unwrap();
}

fn bench_op_scalar<T>(arr_a: &PrimitiveArray<T>, value_b: T, op: Operator)
where
    T: NativeType + std::cmp::PartialOrd,
{
    primtive_compare_scalar(
        criterion::black_box(arr_a),
        criterion::black_box(value_b),
        op,
    )
    .unwrap();
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let arr_a = create_primitive_array::<f32>(size, DataType::Float32, 0.0);
    let arr_b = create_primitive_array::<f32>(size, DataType::Float32, 0.0);

    c.bench_function("eq Float32", |b| {
        b.iter(|| bench_op(&arr_a, &arr_b, Operator::Eq))
    });
    c.bench_function("eq scalar Float32", |b| {
        b.iter(|| bench_op_scalar(&arr_a, 1.0, Operator::Eq))
    });

    c.bench_function("lt Float32", |b| {
        b.iter(|| bench_op(&arr_a, &arr_b, Operator::Lt))
    });
    c.bench_function("lt scalar Float32", |b| {
        b.iter(|| bench_op_scalar(&arr_a, 1.0, Operator::Lt))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
