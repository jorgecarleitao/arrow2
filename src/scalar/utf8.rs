use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

/// The implementation of [`Scalar`] for utf8, semantically equivalent to [`Option<&str>`].
#[derive(Debug, Clone)]
pub struct Utf8Scalar<O: Offset> {
    value: Buffer<u8>, // safety: valid utf8
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> PartialEq for Utf8Scalar<O> {
    fn eq(&self, other: &Self) -> bool {
        self.is_valid == other.is_valid && ((!self.is_valid) | (self.value == other.value))
    }
}

impl<O: Offset> Utf8Scalar<O> {
    /// Returns a new [`Utf8Scalar`]
    #[inline]
    pub fn new<P: AsRef<str>>(v: Option<P>) -> Self {
        let is_valid = v.is_some();
        O::from_usize(v.as_ref().map(|x| x.as_ref().len()).unwrap_or_default()).expect("Too large");
        let value = Buffer::from(v.as_ref().map(|x| x.as_ref().as_bytes()).unwrap_or(&[]));
        Self {
            value,
            is_valid,
            phantom: std::marker::PhantomData,
        }
    }

    /// Returns the value irrespectively of the validity.
    #[inline]
    pub fn value(&self) -> &str {
        // Safety: invariant of the struct
        unsafe { std::str::from_utf8_unchecked(self.value.as_slice()) }
    }
}

impl<O: Offset, P: AsRef<str>> From<Option<P>> for Utf8Scalar<O> {
    #[inline]
    fn from(v: Option<P>) -> Self {
        Self::new(v)
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
}
