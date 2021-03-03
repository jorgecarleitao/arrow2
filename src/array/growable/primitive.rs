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
    array::{Array, PrimitiveArray},
    buffer::{MutableBitmap, MutableBuffer},
    types::NativeType,
};

use super::{
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// A growable PrimitiveArray
pub struct GrowablePrimitive<'a, T: NativeType> {
    arrays: Vec<&'a PrimitiveArray<T>>,
    validity: MutableBitmap,
    values: MutableBuffer<T>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a, T: NativeType> GrowablePrimitive<'a, T> {
    /// # Panics
    /// This function panics if any of the `arrays` is not downcastable to `PrimitiveArray<T>`.
    pub fn new(arrays: &[&'a dyn Array], mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let arrays = arrays
            .iter()
            .map(|array| array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap())
            .collect::<Vec<_>>();

        Self {
            arrays,
            values: MutableBuffer::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> PrimitiveArray<T> {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);

        PrimitiveArray::<T>::from_data(
            self.arrays[0].data_type().clone(),
            values.into(),
            validity.into(),
        )
    }
}

impl<'a, T: NativeType> Growable<'a> for GrowablePrimitive<'a, T> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let values = self.arrays[index].values();
        self.values.extend_from_slice(&values[start..start + len]);
    }

    #[inline]
    fn extend_validity(&mut self, additional: usize) {
        self.values
            .resize(self.values.len() + additional, T::default());
        self.validity.extend_constant(additional, false);
    }

    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn to_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, T: NativeType> Into<PrimitiveArray<T>> for GrowablePrimitive<'a, T> {
    fn into(self) -> PrimitiveArray<T> {
        PrimitiveArray::<T>::from_data(
            self.arrays[0].data_type().clone(),
            self.values.into(),
            self.validity.into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Primitive;
    use crate::datatypes::DataType;

    /// tests extending from a primitive array w/ offset nor nulls
    #[test]
    fn test_primitive() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let mut a = GrowablePrimitive::new(&[&b], false, 3);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(1), Some(2)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    /// tests extending from a primitive array with offset w/ nulls
    #[test]
    fn test_primitive_offset() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], false, 2);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(2), Some(3)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    /// tests extending from a primitive array with offset and nulls
    #[test]
    fn test_primitive_null_offset() {
        let b = Primitive::<u8>::from(vec![Some(1), None, Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], false, 2);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![None, Some(3)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_primitive_null_offset_validity() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], true, 2);
        a.extend(0, 0, 2);
        a.extend_validity(3);
        a.extend(0, 1, 1);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(2), Some(3), None, None, None, Some(3)])
            .to(DataType::UInt8);
        assert_eq!(result, expected);
    }
}
