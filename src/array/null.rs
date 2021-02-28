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

use crate::{buffer::Bitmap, datatypes::DataType};

use super::{ffi::ToFFI, Array};

#[derive(Debug, Clone)]
pub struct NullArray {
    data_type: DataType,
    length: usize,
    offset: usize,
}

impl NullArray {
    pub fn new_empty() -> Self {
        Self::from_data(0)
    }

    /// Returns a new null array
    pub fn new_null(length: usize) -> Self {
        Self::from_data(length)
    }

    pub fn from_data(length: usize) -> Self {
        Self {
            data_type: DataType::Null,
            length,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            length,
            offset: self.offset + offset,
        }
    }
}

impl Array for NullArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    fn validity(&self) -> &Option<Bitmap> {
        &None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for NullArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NullArray({})", self.len())
    }
}

unsafe impl ToFFI for NullArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [None, None, None]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}
