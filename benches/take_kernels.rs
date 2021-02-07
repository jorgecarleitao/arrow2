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

use rand::distributions::{Distribution, Standard};
use rand::{rngs::StdRng, Rng, SeedableRng};

use arrow2::{array::*, datatypes::DataType, datatypes::PrimitiveType};
use arrow2::{compute::take, datatypes::Int32Type};

/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

// cast array from specified primitive array type to desired data type
fn create_primitive<T>(size: usize) -> PrimitiveArray<T::Native>
where
    T: PrimitiveType,
    Standard: Distribution<T::Native>,
{
    let a = seedable_rng()
        .sample_iter(&Standard)
        .take(size)
        .map(Some)
        .collect::<Vec<_>>();
    (T::DATA_TYPE, a).into()
}

fn create_random_index(size: usize, null_density: f32) -> PrimitiveArray<i32> {
    let mut rng = seedable_rng();
    let a = (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _, _>(0i32, size as i32);
                Some(value)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (DataType::Int32, &a).into()
}

fn bench_take(values: &dyn Array, indices: &PrimitiveArray<i32>) {
    criterion::black_box(take::take(values, &indices, None).unwrap());
}

fn add_benchmark(c: &mut Criterion) {
    let values = create_primitive::<Int32Type>(512);
    let indices = create_random_index(512, 0.0);
    c.bench_function("take i32 512", |b| b.iter(|| bench_take(&values, &indices)));
    let values = create_primitive::<Int32Type>(1024);
    let indices = create_random_index(1024, 0.0);
    c.bench_function("take i32 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });

    let indices = create_random_index(512, 0.5);
    c.bench_function("take i32 nulls 512", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
    let values = create_primitive::<Int32Type>(1024);
    let indices = create_random_index(1024, 0.5);
    c.bench_function("take i32 nulls 1024", |b| {
        b.iter(|| bench_take(&values, &indices))
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
