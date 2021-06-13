use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct Utf8Scalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> Utf8Scalar<O> {
    #[inline]
    pub fn new(v: Option<&str>) -> Self {
        let is_valid = v.is_some();
        O::from_usize(v.map(|x| x.len()).unwrap_or_default()).expect("Too large");
        let value = Buffer::from(v.map(|x| x.as_bytes()).unwrap_or(&[]));
        Self {
            value,
            is_valid,
            phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn value(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.value.as_slice()) }
    }
}

impl<O: Offset> Scalar for Utf8Scalar<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        if O::is_large() {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let item_length = O::from_usize(self.value.len()).unwrap(); // verified at `new`
            let offsets = (0..=length).map(|i| O::from_usize(i).unwrap() * item_length);
            let offsets = unsafe { Buffer::from_trusted_len_iter_unchecked(offsets) };
            let values = std::iter::repeat(self.value.as_slice())
                .take(length)
                .flatten()
                .copied()
                .collect();
            Box::new(Utf8Array::<O>::from_data(offsets, values, None))
        } else {
            Box::new(Utf8Array::<O>::new_null(length))
        }
    }
}
