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

use super::BinaryArray;
use crate::array::{Array, Builder, ToArray, TryFromIterator};
use crate::{
    array::Offset,
    buffer::{MutableBitmap, MutableBuffer},
};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result as ArrowResult},
};

impl<O: Offset> BinaryArray<O> {
    pub fn from_slice<T: AsRef<[u8]>, P: AsRef<[T]>>(slice: P) -> Self {
        Self::from_iter(slice.as_ref().iter().map(Some))
    }
}

impl<O: Offset, T: AsRef<[u8]>> From<&Vec<Option<T>>> for BinaryArray<O> {
    fn from(slice: &Vec<Option<T>>) -> Self {
        Self::from_iter(slice.iter().map(|x| x.as_ref()))
    }
}

/// auxiliary struct used to create a [`BinaryArray`] out of an iterator
#[derive(Debug)]
pub struct BinaryPrimitive<O: Offset> {
    offsets: MutableBuffer<O>,
    values: MutableBuffer<u8>,
    validity: MutableBitmap,
    // invariant: always equal to the last offset
    length: O,
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryPrimitive<O> {
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        Self::try_from_iter(iter.into_iter().map(|x| Ok(x))).unwrap()
    }
}

impl<O: Offset, P> TryFromIterator<Option<P>> for BinaryPrimitive<O>
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

impl<O: Offset> Builder<&[u8]> for BinaryPrimitive<O> {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        let mut offsets = MutableBuffer::<O>::with_capacity(capacity + 1);
        let length = O::default();
        unsafe { offsets.push_unchecked(length) };

        Self {
            offsets,
            values: MutableBuffer::<u8>::new(),
            validity: MutableBitmap::with_capacity(capacity),
            length,
        }
    }

    #[inline]
    fn try_push(&mut self, value: Option<&&[u8]>) -> ArrowResult<()> {
        match value {
            Some(v) => {
                let bytes = *v;
                let length =
                    O::from_usize(bytes.len()).ok_or(ArrowError::DictionaryKeyOverflowError)?;
                self.length += length;
                self.offsets.push(self.length);
                self.values.extend_from_slice(bytes);
                self.validity.push(true);
            }
            None => {
                self.offsets.push(self.length);
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

impl<O: Offset> BinaryPrimitive<O> {
    pub fn to(self) -> BinaryArray<O> {
        BinaryArray::<O>::from_data(
            self.offsets.into(),
            self.values.into(),
            self.validity.into(),
        )
    }
}

impl<O: Offset> ToArray for BinaryPrimitive<O> {
    fn to_arc(self, _: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to())
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<Option<P>> for BinaryArray<O> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        BinaryPrimitive::from_iter(iter).to()
    }
}
