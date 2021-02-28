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

use std::{iter::FromIterator, sync::Arc};

use super::FixedSizeBinaryArray;
use crate::array::{Array, Builder, ToArray, TryFromIterator};
use crate::buffer::{MutableBitmap, MutableBuffer};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result as ArrowResult},
};

/// auxiliary struct used to create a [`BinaryArray`] out of an iterator
#[derive(Debug)]
pub struct FixedSizeBinaryPrimitive {
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
    size: Option<usize>,
    current_validity: usize,
}

impl<P: AsRef<[u8]>> FromIterator<Option<P>> for FixedSizeBinaryPrimitive {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(|x| Ok(x))).unwrap()
    }
}

impl<P> TryFromIterator<Option<P>> for FixedSizeBinaryPrimitive
where
    P: AsRef<[u8]>,
{
    fn try_from_iter<I: IntoIterator<Item = ArrowResult<Option<P>>>>(iter: I) -> ArrowResult<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut primitive = Self::with_capacity(lower);
        for item in iterator {
            match item? {
                Some(x) => primitive.try_push(Some(&x.as_ref()))?,
                None => primitive.try_push(None)?,
            }
        }
        Ok(primitive)
    }
}

impl Builder<&[u8]> for FixedSizeBinaryPrimitive {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBuffer::<u8>::new(),
            validity: MutableBitmap::with_capacity(capacity),
            size: None,
            current_validity: 0,
        }
    }

    #[inline]
    fn try_push(&mut self, value: Option<&&[u8]>) -> ArrowResult<()> {
        match value {
            Some(v) => {
                let bytes = *v;
                if let Some(size) = self.size {
                    if size != bytes.len() {
                        return Err(ArrowError::DictionaryKeyOverflowError);
                    }
                } else {
                    self.size = Some(bytes.len());
                    self.values
                        .extend_from_slice(&vec![0; bytes.len() * self.current_validity]);
                };
                self.values.extend_from_slice(bytes);
                self.validity.push(true);
            }
            None => {
                if let Some(size) = self.size {
                    self.values.extend_from_slice(&vec![0; size]);
                } else {
                    self.current_validity += 1;
                }
                self.validity.push(false);
            }
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, value: Option<&&[u8]>) {
        self.try_push(value).unwrap()
    }
}

impl FixedSizeBinaryPrimitive {
    pub fn to(mut self, data_type: DataType) -> FixedSizeBinaryArray {
        let size = *FixedSizeBinaryArray::get_size(&data_type) as usize;
        if let Some(self_size) = self.size {
            assert_eq!(size, self_size);
        } else {
            self.values
                .extend_from_slice(&vec![0; size * self.current_validity])
        };

        FixedSizeBinaryArray::from_data(data_type, self.values.into(), self.validity.into())
    }
}

impl ToArray for FixedSizeBinaryPrimitive {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn basic() {
        let array =
            FixedSizeBinaryPrimitive::from_iter(vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")])
                .to(DataType::FixedSizeBinary(2));
        assert_eq!(array.len(), 4);
    }
}
