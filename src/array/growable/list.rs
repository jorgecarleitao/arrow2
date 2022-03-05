use std::sync::Arc;

use crate::{
    array::{Array, ListArray, Offset},
    bitmap::MutableBitmap,
};

use super::{
    make_growable,
    utils::{build_extend_null_bits, extend_offsets, ExtendNullBits},
    Growable,
};

fn extend_offset_values<O: Offset>(
    growable: &mut GrowableList<'_, O>,
    index: usize,
    start: usize,
    len: usize,
) {
    let array = growable.arrays[index];
    let offsets = array.offsets();

    if array.null_count() == 0 {
        // offsets
        extend_offsets::<O>(
            &mut growable.offsets,
            &mut growable.last_offset,
            &offsets[start..start + len + 1],
        );

        let end = offsets[start + len].to_usize();
        let start = offsets[start].to_usize();
        let len = end - start;
        growable.values.extend(index, start, len)
    } else {
        growable.offsets.reserve(len);

        let new_offsets = &mut growable.offsets;
        let inner_values = &mut growable.values;
        let last_offset = &mut growable.last_offset;
        (start..start + len).for_each(|i| {
            if array.is_valid(i) {
                let len = offsets[i + 1] - offsets[i];
                // compute the new offset
                *last_offset += len;

                // append value
                inner_values.extend(index, offsets[i].to_usize(), len.to_usize());
            }
            // append offset
            new_offsets.push(*last_offset);
        })
    }
}

/// Concrete [`Growable`] for the [`ListArray`].
pub struct GrowableList<'a, O: Offset> {
    arrays: Vec<&'a ListArray<O>>,
    validity: MutableBitmap,
    values: Box<dyn Growable<'a> + 'a>,
    offsets: Vec<O>,
    last_offset: O, // always equal to the last offset at `offsets`.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a, O: Offset> GrowableList<'a, O> {
    /// Creates a new [`GrowableList`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a ListArray<O>>, mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if !use_validity & arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let inner = arrays
            .iter()
            .map(|array| array.values().as_ref())
            .collect::<Vec<_>>();
        let values = make_growable(&inner, use_validity, 0);

        let mut offsets = Vec::with_capacity(capacity + 1);
        let length = O::default();
        offsets.push(length);

        Self {
            arrays,
            offsets,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            last_offset: O::default(),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> ListArray<O> {
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = self.values.as_arc();

        ListArray::<O>::new(
            self.arrays[0].data_type().clone(),
            offsets.into(),
            values,
            validity.into(),
        )
    }
}

impl<'a, O: Offset> Growable<'a> for GrowableList<'a, O> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);
        extend_offset_values::<O>(self, index, start, len);
    }

    fn extend_validity(&mut self, additional: usize) {
        self.offsets
            .resize(self.offsets.len() + additional, self.last_offset);
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, O: Offset> From<GrowableList<'a, O>> for ListArray<O> {
    fn from(val: GrowableList<'a, O>) -> Self {
        let mut values = val.values;
        let values = values.as_arc();

        ListArray::<O>::new(
            val.arrays[0].data_type().clone(),
            val.offsets.into(),
            values,
            val.validity.into(),
        )
    }
}
