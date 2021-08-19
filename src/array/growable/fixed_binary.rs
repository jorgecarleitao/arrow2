use std::sync::Arc;

use crate::{
    array::{Array, FixedSizeBinaryArray},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
};

use super::{
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// Concrete [`Growable`] for the [`FixedSizeBinaryArray`].
pub struct GrowableFixedSizeBinary<'a> {
    arrays: Vec<&'a FixedSizeBinaryArray>,
    validity: MutableBitmap,
    values: MutableBuffer<u8>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
    size: usize, // just a cache
}

impl<'a> GrowableFixedSizeBinary<'a> {
    pub fn new(
        arrays: Vec<&'a FixedSizeBinaryArray>,
        mut use_validity: bool,
        capacity: usize,
    ) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let size = *FixedSizeBinaryArray::get_size(arrays[0].data_type()) as usize;
        Self {
            arrays,
            values: MutableBuffer::with_capacity(0),
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
            size,
        }
    }

    fn to(&mut self) -> FixedSizeBinaryArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);

        FixedSizeBinaryArray::from_data(
            self.arrays[0].data_type().clone(),
            values.into(),
            validity.into(),
        )
    }
}

impl<'a> Growable<'a> for GrowableFixedSizeBinary<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        let values = array.values();

        self.values
            .extend_from_slice(&values[start * self.size..(start + len) * self.size]);
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values.extend_constant(self.size * additional, 0);
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableFixedSizeBinary<'a>> for FixedSizeBinaryArray {
    fn from(val: GrowableFixedSizeBinary<'a>) -> Self {
        FixedSizeBinaryArray::from_data(
            val.arrays[0].data_type().clone(),
            val.values.into(),
            val.validity.into(),
        )
    }
}
