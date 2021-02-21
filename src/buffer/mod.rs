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

//! This module contains the core functionality to handle memory in this crate.
//! It contains two main APIs that to, a large extend, are replacements of Rust's `Vec` for all types
//! supported by this crate.
//!
//! The first set of APIs are [`MutableBuffer`] and [`Buffer`]. These are generics over `T: `[`NativeType`].
//! [`NativeType`] is the trait implemented to all Rust types that this crate supports.
//! [`MutableBuffer`] is like `Vec`, with the main difference that it only supports `NativeType` and
//! allocates memory along cache lines. [`Buffer`] is its imutable counterpart and holds an `Arc` to an
//! imutable memory region.
//!
//! The second set of APIs are [`MutableBitmap`] and [`Bitmap`]. These are containers specifically used
//! to store and handle bitmap operations that the arrow format leverages.
//!
//! Together, these declare all data (not metadata) that is stored in memory by this crate.

mod alignment;
mod alloc;
mod bitmap;
mod bitmap_ops;
mod immutable;
mod mutable;

pub(crate) mod bytes;
pub(crate) mod types;
pub(crate) mod util;

pub use bitmap::Bitmap;
pub use bitmap::MutableBitmap;
pub use bitmap_ops::*;
pub use immutable::Buffer;
pub use mutable::MutableBuffer;
pub use types::{days_ms, NativeType};
