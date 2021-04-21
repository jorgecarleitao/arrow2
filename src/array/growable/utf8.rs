use std::sync::Arc;

use crate::{
    array::{Array, Offset, Utf8Array},
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
};

use super::{
    utils::{build_extend_null_bits, extend_offset_values, extend_offsets, ExtendNullBits},
    Growable,
};

pub struct GrowableUtf8<'a, O: Offset> {
    arrays: Vec<&'a Utf8Array<O>>,
    validity: MutableBitmap,
    values: MutableBuffer<u8>,
    offsets: MutableBuffer<O>,
    length: O, // always equal to the last offset at `offsets`.
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a, O: Offset> GrowableUtf8<'a, O> {
    pub fn new(arrays: &[&'a Utf8Array<O>], mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
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
            arrays: arrays.to_vec(),
            values: MutableBuffer::with_capacity(0),
            offsets,
            length,
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> Utf8Array<O> {
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = std::mem::take(&mut self.values);

        unsafe {
            Utf8Array::<O>::from_data_unchecked(offsets.into(), values.into(), validity.into())
        }
    }
}

impl<'a, O: Offset> Growable<'a> for GrowableUtf8<'a, O> {
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

    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn to_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, O: Offset> From<GrowableUtf8<'a, O>> for Utf8Array<O> {
    fn from(val: GrowableUtf8<'a, O>) -> Self {
        unsafe {
            Utf8Array::<O>::from_data_unchecked(
                val.offsets.into(),
                val.values.into(),
                val.validity.into(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;

    use crate::array::Utf8Array;

    /// tests extending from a variable-sized (strings and binary) array w/ offset with nulls
    #[test]
    fn test_variable_sized_validity() {
        let array = Utf8Array::<i32>::from_iter(vec![Some("a"), Some("bc"), None, Some("defh")]);

        let mut a = GrowableUtf8::new(&[&array], false, 0);

        a.extend(0, 1, 2);

        let result: Utf8Array<i32> = a.into();

        let expected = Utf8Array::<i32>::from_iter(vec![Some("bc"), None]);
        assert_eq!(result, expected);
    }

    /// tests extending from a variable-sized (strings and binary) array
    /// with an offset and nulls
    #[test]
    fn test_variable_sized_offsets() {
        let array = Utf8Array::<i32>::from_iter(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let mut a = GrowableUtf8::new(&[&array], false, 0);

        a.extend(0, 0, 3);

        let result: Utf8Array<i32> = a.into();

        let expected = Utf8Array::<i32>::from_iter(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_offsets() {
        let array = Utf8Array::<i32>::from_iter(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let mut a = GrowableUtf8::new(&[&array], false, 0);

        a.extend(0, 0, 3);

        let result: Utf8Array<i32> = a.into();

        let expected = Utf8Array::<i32>::from_iter(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_multiple_with_validity() {
        let array1 = Utf8Array::<i32>::from_slice(vec!["hello", "world"]);
        let array2 = Utf8Array::<i32>::from_iter(vec![Some("1"), None]);

        let mut a = GrowableUtf8::new(&[&array1, &array2], false, 5);

        a.extend(0, 0, 2);
        a.extend(1, 0, 2);

        let result: Utf8Array<i32> = a.into();

        let expected =
            Utf8Array::<i32>::from_iter(vec![Some("hello"), Some("world"), Some("1"), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_null_offset_validity() {
        let array = Utf8Array::<i32>::from_iter(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let mut a = GrowableUtf8::new(&[&array], true, 0);

        a.extend(0, 1, 2);
        a.extend_validity(1);

        let result: Utf8Array<i32> = a.into();

        let expected = Utf8Array::<i32>::from_iter(vec![None, Some("defh"), None]);
        assert_eq!(result, expected);
    }
}
