use std::sync::Arc;

use crate::{
    array::{Array, StructArray},
    bitmap::MutableBitmap,
};

use super::{
    make_growable,
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// Concrete [`Growable`] for the [`StructArray`].
pub struct GrowableStruct<'a> {
    arrays: Vec<&'a StructArray>,
    validity: MutableBitmap,
    values: Vec<Box<dyn Growable<'a> + 'a>>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> GrowableStruct<'a> {
    /// # Panics
    /// This function panics if any of the `arrays` is not downcastable to `PrimitiveArray<T>`.
    pub fn new(arrays: Vec<&'a StructArray>, mut use_validity: bool, capacity: usize) -> Self {
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
            .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap())
            .collect::<Vec<_>>();

        // ([field1, field2], [field3, field4]) -> ([field1, field3], [field2, field3])
        let values = (0..arrays[0].values().len())
            .map(|i| {
                make_growable(
                    &arrays
                        .iter()
                        .map(|x| x.values()[i].as_ref())
                        .collect::<Vec<_>>(),
                    use_validity,
                    capacity,
                )
            })
            .collect::<Vec<Box<dyn Growable>>>();

        Self {
            arrays,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> StructArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);
        let values = values.into_iter().map(|mut x| x.as_arc()).collect();

        StructArray::from_data(self.arrays[0].fields().to_vec(), values, validity.into())
    }
}

impl<'a> Growable<'a> for GrowableStruct<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        if array.null_count() == 0 {
            self.values
                .iter_mut()
                .for_each(|child| child.extend(index, start, len))
        } else {
            (start..start + len).for_each(|i| {
                if array.is_valid(i) {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend(index, i, 1))
                } else {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend_validity(1))
                }
            })
        }
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values
            .iter_mut()
            .for_each(|child| child.extend_validity(additional));
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableStruct<'a>> for StructArray {
    fn from(val: GrowableStruct<'a>) -> Self {
        let values = val.values.into_iter().map(|mut x| x.as_arc()).collect();

        StructArray::from_data(val.arrays[0].fields().to_vec(), values, val.validity.into())
    }
}
