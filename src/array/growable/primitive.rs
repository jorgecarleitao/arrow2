use std::sync::Arc;

use crate::{
    array::{Array, PrimitiveArray},
    bitmap::{Bitmap, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    types::NativeType,
};

use super::{utils::extend_validity, Growable};

/// Concrete [`Growable`] for the [`PrimitiveArray`].
pub struct GrowablePrimitive<'a, T: NativeType> {
    data_type: DataType,
    arrays: Vec<&'a [T]>,
    validities: Vec<&'a Option<Bitmap>>,
    use_validity: bool,
    validity: MutableBitmap,
    values: MutableBuffer<T>,
}

impl<'a, T: NativeType> GrowablePrimitive<'a, T> {
    pub fn new(
        arrays: Vec<&'a PrimitiveArray<T>>,
        mut use_validity: bool,
        capacity: usize,
    ) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if !use_validity & arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let data_type = arrays[0].data_type().clone();
        let validities = arrays
            .iter()
            .map(|array| array.validity())
            .collect::<Vec<_>>();
        let arrays = arrays
            .iter()
            .map(|array| array.values().as_slice())
            .collect::<Vec<_>>();

        Self {
            data_type,
            arrays,
            validities,
            use_validity,
            values: MutableBuffer::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    #[inline]
    fn to(&mut self) -> PrimitiveArray<T> {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);

        PrimitiveArray::<T>::from_data(self.data_type.clone(), values.into(), validity.into())
    }
}

impl<'a, T: NativeType> Growable<'a> for GrowablePrimitive<'a, T> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let validity = self.validities[index];
        extend_validity(&mut self.validity, validity, start, len, self.use_validity);

        let values = self.arrays[index];
        self.values.extend_from_slice(&values[start..start + len]);
    }

    #[inline]
    fn extend_validity(&mut self, additional: usize) {
        self.values
            .resize(self.values.len() + additional, T::default());
        self.validity.extend_constant(additional, false);
    }

    #[inline]
    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    #[inline]
    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, T: NativeType> From<GrowablePrimitive<'a, T>> for PrimitiveArray<T> {
    #[inline]
    fn from(val: GrowablePrimitive<'a, T>) -> Self {
        PrimitiveArray::<T>::from_data(val.data_type, val.values.into(), val.validity.into())
    }
}
