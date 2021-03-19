use std::sync::Arc;

use crate::{
    array::{Array, PrimitiveArray},
    bitmap::{Bitmap, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    types::NativeType,
};

use super::{utils::extend_validity, Growable};

/// A growable PrimitiveArray
pub struct GrowablePrimitive<'a, T: NativeType> {
    data_type: DataType,
    arrays: Vec<&'a [T]>,
    validities: Vec<&'a Option<Bitmap>>,
    use_validity: bool,
    validity: MutableBitmap,
    values: MutableBuffer<T>,
}

impl<'a, T: NativeType> GrowablePrimitive<'a, T> {
    pub fn new(arrays: &[&'a PrimitiveArray<T>], mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let data_type = arrays[0].data_type().clone();
        let validities = arrays
            .iter()
            .map(|array| array.validity())
            .collect::<Vec<_>>();
        let arrays = arrays
            .iter()
            .map(|array| array.values())
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
    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    #[inline]
    fn to_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, T: NativeType> Into<PrimitiveArray<T>> for GrowablePrimitive<'a, T> {
    #[inline]
    fn into(self) -> PrimitiveArray<T> {
        PrimitiveArray::<T>::from_data(self.data_type, self.values.into(), self.validity.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Primitive;
    use crate::datatypes::DataType;

    /// tests extending from a primitive array w/ offset nor nulls
    #[test]
    fn test_primitive() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let mut a = GrowablePrimitive::new(&[&b], false, 3);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(1), Some(2)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    /// tests extending from a primitive array with offset w/ nulls
    #[test]
    fn test_primitive_offset() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], false, 2);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(2), Some(3)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    /// tests extending from a primitive array with offset and nulls
    #[test]
    fn test_primitive_null_offset() {
        let b = Primitive::<u8>::from(vec![Some(1), None, Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], false, 2);
        a.extend(0, 0, 2);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![None, Some(3)]).to(DataType::UInt8);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_primitive_null_offset_validity() {
        let b = Primitive::<u8>::from(vec![Some(1), Some(2), Some(3)]).to(DataType::UInt8);
        let b = b.slice(1, 2);
        let mut a = GrowablePrimitive::new(&[&b], true, 2);
        a.extend(0, 0, 2);
        a.extend_validity(3);
        a.extend(0, 1, 1);
        let result: PrimitiveArray<u8> = a.into();
        let expected = Primitive::<u8>::from(vec![Some(2), Some(3), None, None, None, Some(3)])
            .to(DataType::UInt8);
        assert_eq!(result, expected);
    }
}
