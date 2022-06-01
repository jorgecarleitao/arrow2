use std::sync::Arc;

use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
};

use super::{
    make_growable,
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// Concrete [`Growable`] for the [`DictionaryArray`].
/// # Implementation
/// This growable does not perform collision checks and instead concatenates
/// the values of each [`DictionaryArray`] one after the other.
pub struct GrowableDictionary<'a, K: DictionaryKey> {
    keys_values: Vec<&'a [K]>,
    key_values: Vec<K>,
    key_validity: MutableBitmap,
    offsets: Vec<usize>,
    values: Box<dyn Array>,
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

fn concatenate_values<K: DictionaryKey>(
    arrays_keys: &[&PrimitiveArray<K>],
    arrays_values: &[&dyn Array],
    capacity: usize,
) -> (Box<dyn Array>, Vec<usize>) {
    let mut mutable = make_growable(arrays_values, false, capacity);
    let mut offsets = Vec::with_capacity(arrays_keys.len() + 1);
    offsets.push(0);
    for (i, values) in arrays_values.iter().enumerate() {
        mutable.extend(i, 0, values.len());
        offsets.push(offsets[i] + values.len());
    }
    (mutable.as_box(), offsets)
}

impl<'a, T: DictionaryKey> GrowableDictionary<'a, T> {
    /// Creates a new [`GrowableDictionary`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: &[&'a DictionaryArray<T>], mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let arrays_keys = arrays.iter().map(|array| array.keys()).collect::<Vec<_>>();
        let keys_values = arrays_keys
            .iter()
            .map(|array| array.values().as_slice())
            .collect::<Vec<_>>();

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(array.keys(), use_validity))
            .collect();

        let arrays_values = arrays
            .iter()
            .map(|array| array.values().as_ref())
            .collect::<Vec<_>>();

        let (values, offsets) = concatenate_values(&arrays_keys, &arrays_values, capacity);

        Self {
            offsets,
            values,
            keys_values,
            key_values: Vec::with_capacity(capacity),
            key_validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    #[inline]
    fn to(&mut self) -> DictionaryArray<T> {
        let validity = std::mem::take(&mut self.key_validity);
        let key_values = std::mem::take(&mut self.key_values);

        let data_type = T::PRIMITIVE.into();
        let keys = PrimitiveArray::<T>::from_data(data_type, key_values.into(), validity.into());

        DictionaryArray::<T>::from_data(keys, self.values.clone())
    }
}

impl<'a, T: DictionaryKey> Growable<'a> for GrowableDictionary<'a, T> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.key_validity, start, len);

        let values = &self.keys_values[index][start..start + len];
        let offset = self.offsets[index];
        self.key_values.extend(
            values
                .iter()
                // `.unwrap_or(0)` because this operation does not check for null values, which may contain any key.
                .map(|x| T::from_usize(offset + x.to_usize().unwrap_or(0)).unwrap()),
        );
    }

    #[inline]
    fn extend_validity(&mut self, additional: usize) {
        self.key_values
            .resize(self.key_values.len() + additional, T::default());
        self.key_validity.extend_constant(additional, false);
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

impl<'a, T: DictionaryKey> From<GrowableDictionary<'a, T>> for DictionaryArray<T> {
    #[inline]
    fn from(val: GrowableDictionary<'a, T>) -> Self {
        let data_type = T::PRIMITIVE.into();
        let keys = PrimitiveArray::<T>::from_data(
            data_type,
            val.key_values.into(),
            val.key_validity.into(),
        );

        DictionaryArray::<T>::from_data(keys, val.values)
    }
}
