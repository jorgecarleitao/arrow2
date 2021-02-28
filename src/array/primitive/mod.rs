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
    buffer::{types::NativeType, Bitmap, Buffer},
    datatypes::DataType,
    error::ArrowError,
    ffi::ArrowArray,
};

use crate::error::Result;

use super::{
    display_fmt,
    ffi::{FromFFI, ToFFI},
    Array,
};

#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    // Returns a new [`PrimitiveArray`] whose validity is all null
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                std::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        Self {
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
        }
    }

    #[inline]
    pub fn values_buffer(&self) -> &Buffer<T> {
        &self.values
    }

    #[inline]
    pub fn values(&self) -> &[T] {
        self.values.as_slice()
    }

    #[inline]
    pub fn value(&self, i: usize) -> T {
        self.values()[i]
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl<T: NativeType> std::fmt::Display for PrimitiveArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: match data_type and format dates
        display_fmt(self.iter(), &format!("{}", self.data_type()), f, false)
    }
}

unsafe impl<T: NativeType> ToFFI for PrimitiveArray<T> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        unsafe {
            [
                self.validity.as_ref().map(|x| x.as_ptr()),
                Some(std::ptr::NonNull::new_unchecked(
                    self.values.as_ptr() as *mut u8
                )),
                None,
            ]
        }
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

unsafe impl<T: NativeType> FromFFI for PrimitiveArray<T> {
    fn try_from_ffi(data_type: DataType, array: ArrowArray) -> Result<Self> {
        let length = array.len();
        let offset = array.offset();
        let mut validity = array.null_bit_buffer();
        let mut values = unsafe { array.buffer::<T>(0)? };

        if offset > 0 {
            values = values.slice(offset, length);
            validity = validity.map(|x| x.slice(offset, length))
        }
        Ok(Self {
            data_type,
            values,
            validity,
            offset: 0,
        })
    }
}

mod from;
pub use from::Primitive;
mod iterator;
pub use iterator::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn display() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)]).to(DataType::Int32);
        assert_eq!(format!("{}", array), "Int32[1, , 2]");
    }

    #[test]
    fn basics() {
        let data = vec![Some(1), None, Some(10)];

        let array = Primitive::<i32>::from_iter(data).to(DataType::Int32);

        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 0);
        assert_eq!(array.value(2), 10);
        assert_eq!(array.values(), &[1, 0, 10]);
        assert_eq!(array.validity(), &Some(Bitmap::from((&[0b00000101], 3))));
        assert_eq!(array.is_valid(0), true);
        assert_eq!(array.is_valid(1), false);
        assert_eq!(array.is_valid(2), true);

        let array2 = PrimitiveArray::<i32>::from_data(
            DataType::Int32,
            array.values_buffer().clone(),
            array.validity().clone(),
        );
        assert_eq!(array, array2);

        let array = array.slice(1, 2);
        assert_eq!(array.value(0), 0);
        assert_eq!(array.value(1), 10);
        assert_eq!(array.values(), &[0, 10]);
    }

    #[test]
    fn empty() {
        let array = PrimitiveArray::<i32>::new_empty(DataType::Int32);
        assert_eq!(array.values().len(), 0);
        assert_eq!(array.validity(), &None);
    }
}
