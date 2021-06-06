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

use arrow2::bitmap::utils::null_count;

use criterion::{criterion_group, criterion_main, Criterion};

fn add_benchmark(c: &mut Criterion) {
    let bytes = (0..1026)
        .map(|x| 0b01011011u8.rotate_left(x))
        .collect::<Vec<_>>();

    c.bench_function("null_count", |b| {
        b.iter(|| null_count(&bytes, 0, bytes.len() * 8))
    });

    c.bench_function("null_count_offset", |b| {
        b.iter(|| null_count(&bytes, 10, bytes.len() * 8 - 10))
    });

    c.bench_function("null_count_sliced", |b| {
        b.iter(|| null_count(&bytes, 10, bytes.len() * 8 - 20))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
