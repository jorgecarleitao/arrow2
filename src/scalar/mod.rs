use std::any::Any;

use crate::{array::*, bitmap::Bitmap, buffer::Buffer, datatypes::DataType, types::NativeType};

pub trait Scalar: std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn is_valid(&self) -> bool;

    fn data_type(&self) -> &DataType;

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array>;
}

#[derive(Debug, Clone)]
pub struct PrimitiveScalar<T: NativeType> {
    // Not Option<T> because this offers a stabler pointer offset on the struct
    value: T,
    is_valid: bool,
    data_type: DataType,
}

impl<T: NativeType> PrimitiveScalar<T> {
    #[inline]
    pub fn new(data_type: DataType, v: Option<T>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
            data_type,
        }
    }

    #[inline]
    pub fn value(&self) -> T {
        self.value
    }
}

impl<T: NativeType> Scalar for PrimitiveScalar<T> {
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
        &self.data_type
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Buffer::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(PrimitiveArray::from_data(
                self.data_type.clone(),
                values,
                None,
            ))
        } else {
            Box::new(PrimitiveArray::<T>::new_null(
                self.data_type.clone(),
                length,
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct BooleanScalar {
    value: bool,
    is_valid: bool,
}

impl BooleanScalar {
    #[inline]
    pub fn new(v: Option<bool>) -> Self {
        let is_valid = v.is_some();
        Self {
            value: v.unwrap_or_default(),
            is_valid,
        }
    }

    #[inline]
    pub fn value(&self) -> bool {
        self.value
    }
}

impl Scalar for BooleanScalar {
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
        &DataType::Boolean
    }

    fn to_boxed_array(&self, length: usize) -> Box<dyn Array> {
        if self.is_valid {
            let values = Bitmap::from_trusted_len_iter(std::iter::repeat(self.value).take(length));
            Box::new(BooleanArray::from_data(values, None))
        } else {
            Box::new(BooleanArray::new_null(length))
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct BinaryScalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> BinaryScalar<O> {
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
    pub fn value(&self) -> &[u8] {
        self.value.as_slice()
    }
}

impl<O: Offset> Scalar for BinaryScalar<O> {
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
            &DataType::LargeBinary
        } else {
            &DataType::Binary
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
            Box::new(BinaryArray::<O>::from_data(offsets, values, None))
        } else {
            Box::new(BinaryArray::<O>::new_null(length))
        }
    }
}
