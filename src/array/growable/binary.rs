use std::sync::Arc;

use crate::{
    array::{Array, BinaryArray, Offset},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::DataType,
};

use super::{
    utils::{build_extend_null_bits, extend_offset_values, extend_offsets, ExtendNullBits},
    Growable,
};

/// Concrete [`Growable`] for the [`BinaryArray`].
pub struct GrowableBinary<'a, O: Offset> {
    arrays: Vec<&'a BinaryArray<O>>,
    data_type: DataType,
    validity: MutableBitmap,
    values: MutableBuffer<u8>,
    offsets: MutableBuffer<O>,
    length: O, // always equal to the last offset at `offsets`.
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a, O: Offset> GrowableBinary<'a, O> {
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a BinaryArray<O>>, mut use_validity: bool, capacity: usize) -> Self {
        let data_type = arrays[0].data_type().clone();

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if !use_validity & arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let mut offsets = MutableBuffer::with_capacity(capacity + 1);
        let length = O::default();
        unsafe { offsets.push_unchecked(length) };

        Self {
            arrays,
            data_type,
            values: MutableBuffer::with_capacity(0),
            offsets,
            length,
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> BinaryArray<O> {
        let data_type = self.data_type.clone();
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = std::mem::take(&mut self.values);

        BinaryArray::<O>::from_data(data_type, offsets.into(), values.into(), validity.into())
    }
}

impl<'a, O: Offset> Growable<'a> for GrowableBinary<'a, O> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        let offsets = array.offsets();
        let values = array.values();

        extend_offsets::<O>(
            &mut self.offsets,
            &mut self.length,
            &offsets[start..start + len + 1],
        );
        // values
        extend_offset_values::<O>(&mut self.values, offsets, values, start, len);
    }

    fn extend_validity(&mut self, additional: usize) {
        self.offsets.extend_constant(additional, self.length);
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, O: Offset> From<GrowableBinary<'a, O>> for BinaryArray<O> {
    fn from(val: GrowableBinary<'a, O>) -> Self {
        BinaryArray::<O>::from_data(
            val.data_type,
            val.offsets.into(),
            val.values.into(),
            val.validity.into(),
        )
    }
}
