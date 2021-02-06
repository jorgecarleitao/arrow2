use std::{borrow::Borrow, iter::FromIterator};

use bits::null_count;

use crate::{
    bits,
    buffers::{Buffer, MutableBuffer},
    datatypes::{DataType, PrimitiveType},
};

use super::Array;

#[derive(Debug)]
pub struct PrimitiveArray<T: PrimitiveType> {
    data_type: DataType,
    values: Buffer<T::Native>,
    validity: Option<Buffer<u8>>,
    null_count: usize,
}

impl<T: PrimitiveType> PrimitiveArray<T> {
    fn from_data(
        data_type: DataType,
        values: Buffer<T::Native>,
        validity: Option<Buffer<u8>>,
    ) -> Self {
        let null_count = null_count(validity.as_ref().map(|x| x.as_slice()), 0, values.len());
        Self {
            data_type,
            values,
            validity,
            null_count,
        }
    }
}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn is_null(&self, _: usize) -> bool {
        todo!()
    }
}

impl<T: PrimitiveType, Ptr: Borrow<Option<<T as PrimitiveType>::Native>>> FromIterator<Ptr>
    for PrimitiveArray<T>
{
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let num_bytes = bits::bytes_for(data_len);
        let mut nulls = MutableBuffer::<u8>::from_len_zeroed(num_bytes);

        let null_slice = nulls.as_slice_mut();
        let values: MutableBuffer<T::Native> = iter
            .enumerate()
            .map(|(i, item)| {
                if let Some(a) = item.borrow() {
                    bits::set_bit(null_slice, i);
                    *a
                } else {
                    T::Native::default()
                }
            })
            .collect();

        Self::from_data(T::DATA_TYPE, values.into(), Some(nulls.into()))
    }
}
