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
    array::{Array, BooleanArray},
    buffer::MutableBitmap,
};

use super::{
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// A growable PrimitiveArray
pub struct GrowableBoolean<'a> {
    arrays: Vec<&'a BooleanArray>,
    validity: MutableBitmap,
    values: MutableBitmap,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> GrowableBoolean<'a> {
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
            .map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap())
            .collect::<Vec<_>>();

        Self {
            arrays,
            values: MutableBitmap::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> BooleanArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);

        BooleanArray::from_data(values.into(), validity.into())
    }
}

impl<'a> Growable<'a> for GrowableBoolean<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        let values = array.values();
        (start..start + len).for_each(|i| self.values.push(values.get_bit(i)));
    }

    fn extend_validity(&mut self, additional: usize) {
        (0..additional).for_each(|_| {
            self.values.push(false);
            self.validity.push(false);
        });
    }

    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn to_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> Into<BooleanArray> for GrowableBoolean<'a> {
    fn into(self) -> BooleanArray {
        BooleanArray::from_data(self.values.into(), self.validity.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool() {
        let array = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);

        let mut a = GrowableBoolean::new(&[&array], false, 0);

        a.extend(0, 1, 2);

        let result: BooleanArray = a.into();

        let expected = BooleanArray::from(vec![Some(true), None]);
        assert_eq!(result, expected);
    }
}
