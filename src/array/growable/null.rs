use std::sync::Arc;

use crate::array::{Array, NullArray};

use super::Growable;

/// A growable PrimitiveArray
pub struct GrowableNull {
    length: usize,
}

impl Default for GrowableNull {
    fn default() -> Self {
        Self { length: 0 }
    }
}

impl GrowableNull {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<'a> Growable<'a> for GrowableNull {
    fn extend(&mut self, _: usize, _: usize, len: usize) {
        self.length += len;
    }

    fn extend_validity(&mut self, additional: usize) {
        self.length += additional;
    }

    fn to_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(NullArray::from_data(self.length))
    }

    fn to_box(&mut self) -> Box<dyn Array> {
        Box::new(NullArray::from_data(self.length))
    }
}

impl Into<NullArray> for GrowableNull {
    fn into(self) -> NullArray {
        NullArray::from_data(self.length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let mut mutable = GrowableNull::new();

        mutable.extend(0, 1, 2);
        mutable.extend(1, 0, 1);

        let result: NullArray = mutable.into();

        let expected = NullArray::from_data(3);
        assert_eq!(result, expected);
    }
}
