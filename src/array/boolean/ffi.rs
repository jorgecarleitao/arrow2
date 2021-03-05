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

use crate::{
    array::{FromFFI, ToFFI},
    datatypes::DataType,
    ffi::ArrowArray,
};

use crate::error::Result;

use super::BooleanArray;

unsafe impl ToFFI for BooleanArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr()),
            None,
        ]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl FromFFI for BooleanArray {
    fn try_from_ffi(_: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.validity();
        let mut values = unsafe { array.bitmap(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self::from_data(values, validity))
    }
}
