use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct BinaryScalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> PartialEq for BinaryScalar<O> {
    fn eq(&self, other: &Self) -> bool {
        self.is_valid == other.is_valid && ((!self.is_valid) | (self.value == other.value))
    }
}

impl<O: Offset> BinaryScalar<O> {
    #[inline]
    pub fn new<P: AsRef<[u8]>>(v: Option<P>) -> Self {
        let is_valid = v.is_some();
        O::from_usize(v.as_ref().map(|x| x.as_ref().len()).unwrap_or_default()).expect("Too large");
        let value = Buffer::from(v.as_ref().map(|x| x.as_ref()).unwrap_or(&[]));
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

impl<O: Offset, P: AsRef<[u8]>> From<Option<P>> for BinaryScalar<O> {
    #[inline]
    fn from(v: Option<P>) -> Self {
        Self::new(v)
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
}
