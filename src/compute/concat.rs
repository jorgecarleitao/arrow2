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

//! Contains the concatenate kernel
//!
//! Example:
//!
//! ```
//! use arrow2::array::Utf8Array;
//! use arrow2::compute::concat::concatenate;
//!
//! let arr = concatenate(&[
//!     &Utf8Array::<i32>::from_slice(vec!["hello", "world"]),
//!     &Utf8Array::<i32>::from_slice(vec!["!"]),
//! ]).unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use crate::array::{growable::make_growable, Array};
use crate::error::{ArrowError, Result};

/// Concatenate multiple [Array] of the same type into a single [`Array`].
pub fn concatenate(arrays: &[&dyn Array]) -> Result<Box<dyn Array>> {
    if arrays.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    if arrays
        .iter()
        .any(|array| array.data_type() != arrays[0].data_type())
    {
        return Err(ArrowError::InvalidArgumentError(
            "It is not possible to concatenate arrays of different data types.".to_string(),
        ));
    }

    let lengths = arrays.iter().map(|array| array.len()).collect::<Vec<_>>();
    let capacity = lengths.iter().sum();

    let mut mutable = make_growable(arrays, false, capacity);

    for (i, len) in lengths.iter().enumerate() {
        mutable.extend(i, 0, *len)
    }

    Ok(mutable.as_box())
}
