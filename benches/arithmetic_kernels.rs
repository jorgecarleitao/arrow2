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
use arrow2::{
    compute::arithmetics::basic::div::div_scalar, datatypes::DataType, types::NativeType,
};
use num_traits::NumCast;
use std::ops::Div;

fn bench_div_scalar<T>(lhs: &PrimitiveArray<T>, rhs: &T)
where
    T: NativeType + Div<Output = T> + NumCast,
{
    criterion::black_box(div_scalar(lhs, rhs));
}

fn add_benchmark(c: &mut Criterion) {
    let size = 65536;
    let arr = create_primitive_array_with_seed::<u64>(size, DataType::UInt64, 0.0, 43);

    c.bench_function("divide_scalar 4", |b| {
        // 4 is a very fast optimizable divisor
        b.iter(|| bench_div_scalar(&arr, &4))
    });
    c.bench_function("divide_scalar prime", |b| {
        // large prime number that is probably harder to simplify
        b.iter(|| bench_div_scalar(&arr, &524287))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
