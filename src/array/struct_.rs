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

use std::sync::Arc;

use crate::{
    buffer::Bitmap,
    datatypes::{DataType, Field},
};

use super::{ffi::ToFFI, new_empty_array, Array};

#[derive(Debug, Clone)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Arc<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
    pub fn new_empty(fields: &[Field]) -> Self {
        let values = fields
            .iter()
            .map(|field| new_empty_array(field.data_type().clone()).into())
            .collect();
        Self::from_data(fields.to_vec(), values, None)
    }

    pub fn from_data(
        fields: Vec<Field>,
        values: Vec<Arc<dyn Array>>,
        validity: Option<Bitmap>,
    ) -> Self {
        assert!(fields.len() > 0);
        assert_eq!(fields.len(), values.len());
        assert!(values.iter().all(|x| x.len() == values[0].len()));
        if let Some(ref validity) = validity {
            assert_eq!(values[0].len(), validity.len());
        }
        Self {
            data_type: DataType::Struct(fields),
            values,
            validity,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self
                .values
                .iter()
                .map(|x| x.slice(offset, length).into())
                .collect(),
            validity,
        }
    }

    #[inline]
    pub fn values(&self) -> &[Arc<dyn Array>] {
        &self.values
    }

    #[inline]
    pub fn fields(&self) -> &[Field] {
        Self::get_fields(&self.data_type)
    }
}

impl StructArray {
    pub fn get_fields(datatype: &DataType) -> &[Field] {
        if let DataType::Struct(fields) = datatype {
            fields
        } else {
            panic!("Wrong datatype passed to Struct.")
        }
    }
}

impl Array for StructArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values[0].len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl std::fmt::Display for StructArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "StructArray{{")?;
        for (field, column) in self.fields().iter().zip(self.values()) {
            writeln!(f, "{}: {},", field.name(), column)?;
        }
        write!(f, "}}")
    }
}

unsafe impl ToFFI for StructArray {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [self.validity.as_ref().map(|x| x.as_ptr()), None, None]
    }

    fn offset(&self) -> usize {
        // we do not support offsets in structs. Instead, if an FFI we slice the incoming arrays
        0
    }
}
