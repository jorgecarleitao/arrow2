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

//! Contains declarations to bind to the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
//!
//! Generally, this module is divided in two main interfaces:
//! One interface maps C ABI to native Rust types, i.e. convert c-pointers, c_char, to native rust.
//! This is handled by [FFI_ArrowSchema] and [FFI_ArrowArray].
//!
//! The second interface maps native Rust types to the Rust-specific implementation of Arrow such as `format` to `Datatype`,
//! `Buffer`, etc. This is handled by `ArrowArray`.
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow2::array::{Array, Primitive, Int32Array};
//! # use arrow2::error::{Result, ArrowError};
//! # use arrow2::ffi::ArrowArray;
//! # use arrow2::datatypes::DataType;
//! # use std::convert::TryFrom;
//! # fn main() -> Result<()> {
//! // create an array natively
//! let array = Arc::new(Primitive::from(&[Some(1i32), None, Some(3)]).to(DataType::Int32)) as Arc<dyn Array>;
//!
//! // export it
//! let array = ArrowArray::try_from(array)?;
//! let (array, schema) = ArrowArray::into_raw(array);
//!
//! // consumed and used by something else...
//!
//! // import it
//! let array = unsafe { ArrowArray::try_from_raw(array, schema) }?;
//! let array = Box::<dyn Array>::try_from(array)?;
//!
//! // perform some operation
//! let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
//!
//! // verify
//! let expected = Primitive::from(&[Some(1i32), None, Some(3)]).to(DataType::Int32);
//! assert_eq!(array, &expected);
//!
//! // (drop/release)
//! Ok(())
//! }
//! ```

mod array;
#[allow(clippy::module_inception)]
mod ffi;

trait ToFFI {
    // necessary for ffi. first must be the bitmap
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3];

    fn offset(&self) -> usize;
}

pub use ffi::ArrowArray;
pub(crate) use ffi::FFI_ArrowArray;
