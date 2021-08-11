use crate::{array::*, buffer::Buffer, datatypes::DataType};

use super::Scalar;

#[derive(Debug, Clone)]
pub struct Utf8Scalar<O: Offset> {
    value: Buffer<u8>,
    is_valid: bool,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> PartialEq for Utf8Scalar<O> {
    fn eq(&self, other: &Self) -> bool {
        self.is_valid == other.is_valid && ((!self.is_valid) | (self.value == other.value))
    }
}

impl<O: Offset> Utf8Scalar<O> {
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

    #[inline]
    pub fn value(&self) -> &str {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::eq_op)]
    #[test]
    fn equal() {
        let a = Utf8Scalar::<i32>::from(Some("a"));
        let b = Utf8Scalar::<i32>::from(None::<&str>);
        assert_eq!(a, a);
        assert_eq!(b, b);
        assert!(a != b);
        let b = Utf8Scalar::<i32>::from(Some("b"));
        assert!(a != b);
        assert_eq!(b, b);
    }

    #[test]
    fn basics() {
        let a = Utf8Scalar::<i32>::from(Some("a"));

        assert_eq!(a.value(), "a");
        assert_eq!(a.data_type(), &DataType::Utf8);
        assert!(a.is_valid());

        let a = Utf8Scalar::<i64>::from(None::<&str>);

        assert_eq!(a.data_type(), &DataType::LargeUtf8);
        assert!(!a.is_valid());
    }
}
