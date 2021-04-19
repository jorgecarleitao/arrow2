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

//! Kernel operations for arrow arrays

//! # Compute module
//!
//! The compute module is composed by independent operations common in
//! analytics
//!
//! ## Design principles
//!
//! Below are the design principles followed to define the compute kernels:
//!
//! * APIs MUST return an error when either:
//!     * The arguments are incorrect
//!     * The execution results in a predictable error (e.g. divide by zero)
//!
//! * APIs MAY error when an operation overflows (e.g. `i32 + i32`)
//!
//! * kernels MUST NOT have side-effects
//!
//! * kernels MUST NOT take ownership of any of its arguments (i.e. everything
//!   must be a reference).
//!
//! * APIs SHOULD error when an operation on variable sized containers can
//!   overflow the maximum size of `usize`.
//!
//! * Kernels SHOULD use the arrays' logical type to decide whether kernels can
//!   be applied on an array. For example, `Date32 + Date32` is meaningless and
//!   SHOULD NOT be implemented.
//!
//! * Kernels SHOULD be implemented via `clone`, `slice` or the `iterator` API
//!   provided by `Buffer`, `Bitmap`, `MutableBuffer` or `MutableBitmap`.
//!
//! * Kernels MUST NOT use any API to read bits other than the ones provided by
//!   `Bitmap`.
//!
//! * Implementations SHOULD aim for auto-vectorization, which is usually
//!   accomplished via `from_trusted_len_iter`.
//!
//! * Implementations MUST feature-gate any implementation that requires
//!   external dependencies
//!
//! * When a kernel accepts dynamically-typed arrays, it MUST expect them as
//!   `&dyn Array`.
//!
//! * When an API returns `&dyn Array`, it MUST return `Box<dyn Array>`. The
//!   rational is that a `Box` is mutable, while an `Arc` is not. As such,
//!   `Box` offers the most flexible API to consumers and the compiler. Users
//!   can cast a `Box` into `Arc` via `.into()`.

pub mod aggregate;
pub mod arithmetics;
pub mod arity;
pub mod boolean;
pub mod boolean_kleene;
pub mod cast;
pub mod comparison;
pub mod concat;
pub mod contains;
pub mod filter;
pub mod hash;
pub mod length;
pub mod limit;
pub mod sort;
pub mod substring;
pub mod take;
pub mod temporal;
mod utils;
pub mod window;

#[cfg(feature = "regex")]
pub mod regex_match;

#[cfg(feature = "merge_sort")]
pub mod merge_sort;
