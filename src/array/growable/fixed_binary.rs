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
    /// # Panics
    /// This function panics if any of the `arrays` is not downcastable to `FixedSizeBinaryArray`.
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
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
            })
            .collect::<Vec<_>>();

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
            .extend_from_slice(&values[start * self.size..start * self.size + len * self.size]);
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values
            .extend_from_slice(&vec![0; self.size * additional]);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// tests extending from a variable-sized (strings and binary) array w/ offset with nulls
    #[test]
    fn basic() {
        let array =
            FixedSizeBinaryArray::from_iter(vec![Some(b"ab"), Some(b"bc"), None, Some(b"de")], 2);

        let mut a = GrowableFixedSizeBinary::new(&[&array], false, 0);

        a.extend(0, 1, 2);

        let result: FixedSizeBinaryArray = a.into();

        let expected = FixedSizeBinaryArray::from_iter(vec![Some("bc"), None], 2);
        assert_eq!(result, expected);
    }

    /// tests extending from a variable-sized (strings and binary) array
    /// with an offset and nulls
    #[test]
    fn offsets() {
        let array =
            FixedSizeBinaryArray::from_iter(vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")], 2);
        let array = array.slice(1, 3);

        let mut a = GrowableFixedSizeBinary::new(&[&array], false, 0);

        a.extend(0, 0, 3);

        let result: FixedSizeBinaryArray = a.into();

        let expected = FixedSizeBinaryArray::from_iter(vec![Some(b"bc"), None, Some(b"fh")], 2);
        assert_eq!(result, expected);
    }

    #[test]
    fn multiple_with_validity() {
        let array1 = FixedSizeBinaryArray::from_iter(vec![Some("hello"), Some("world")], 5);
        let array2 = FixedSizeBinaryArray::from_iter(vec![Some("12345"), None], 5);

        let mut a = GrowableFixedSizeBinary::new(&[&array1, &array2], false, 5);

        a.extend(0, 0, 2);
        a.extend(1, 0, 2);

        let result: FixedSizeBinaryArray = a.into();

        let expected = FixedSizeBinaryArray::from_iter(
            vec![Some("hello"), Some("world"), Some("12345"), None],
            5,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn null_offset_validity() {
        let array =
            FixedSizeBinaryArray::from_iter(vec![Some("aa"), Some("bc"), None, Some("fh")], 2);
        let array = array.slice(1, 3);

        let mut a = GrowableFixedSizeBinary::new(&[&array], true, 0);

        a.extend(0, 1, 2);
        a.extend_validity(1);

        let result: FixedSizeBinaryArray = a.into();

        let expected = FixedSizeBinaryArray::from_iter(vec![None, Some("fh"), None], 2);
        assert_eq!(result, expected);
    }

    #[test]
    fn sized_offsets() {
        let array =
            FixedSizeBinaryArray::from_iter(vec![Some(&[0, 0]), Some(&[0, 1]), Some(&[0, 2])], 2);
        let array = array.slice(1, 2);
        // = [[0, 1], [0, 2]] due to the offset = 1

        let mut a = GrowableFixedSizeBinary::new(&[&array], false, 0);

        a.extend(0, 1, 1);
        a.extend(0, 0, 1);

        let result: FixedSizeBinaryArray = a.into();

        let expected = FixedSizeBinaryArray::from_iter(vec![Some(&[0, 2]), Some(&[0, 1])], 2);
        assert_eq!(result, expected);
    }
}
