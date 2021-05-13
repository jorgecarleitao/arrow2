use std::sync::Arc;

use crate::{
    array::{Array, BooleanArray},
    bitmap::MutableBitmap,
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
        let iter = (start..start + len).map(|i| values.get_bit(i));
        unsafe { self.values.extend_from_trusted_len_iter_unchecked(iter) };
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values.extend_constant(additional, false);
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableBoolean<'a>> for BooleanArray {
    fn from(val: GrowableBoolean<'a>) -> Self {
        BooleanArray::from_data(val.values.into(), val.validity.into())
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
