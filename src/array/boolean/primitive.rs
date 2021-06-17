use std::iter::FromIterator;
use std::sync::Arc;

use crate::array::{IntoArray, TryExtend};
use crate::array::{NullableBuilder, TryFromIterator};
use crate::error::Result;
use crate::{
    array::{Array, BooleanArray, Builder},
    bitmap::MutableBitmap,
};

/// Auxiliary struct used to create a [`BooleanArray`] incrementally.
/// Contrarily to `BooleanArray` constructors, this struct supports incrementing itself by
/// single (optional) elements.
/// The tradeoff is that this struct is not clonable nor `Send + Sync`.
#[derive(Debug)]
pub struct BooleanPrimitive {
    values: MutableBitmap,
    validity: MutableBitmap,
}

impl NullableBuilder for BooleanPrimitive {
    #[inline]
    fn push_null(&mut self) {
        self.values.push(false);
        self.validity.push(false);
    }
}

impl Builder<bool> for BooleanPrimitive {
    /// Pushes a new item to this struct
    #[inline]
    fn push(&mut self, value: bool) {
        self.values.push(value);
        self.validity.push(true);
    }
}

impl BooleanPrimitive {
    /// Initializes itself with a capacity.
    #[inline]
    pub fn new() -> Self {
        Self {
            values: MutableBitmap::new(),
            validity: MutableBitmap::new(),
        }
    }
}

impl<Ptr: std::borrow::Borrow<Option<bool>>> FromIterator<Ptr> for BooleanPrimitive {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity = MutableBitmap::with_capacity(lower);

        let values: MutableBitmap = iter
            .map(|item| {
                if let Some(a) = item.borrow() {
                    validity.push(true);
                    *a
                } else {
                    validity.push(false);
                    false
                }
            })
            .collect();

        Self { values, validity }
    }
}

impl Default for BooleanPrimitive {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<Ptr: std::borrow::Borrow<Option<bool>>> TryExtend<Ptr> for BooleanPrimitive {
    fn try_extend<T: IntoIterator<Item = Ptr>>(&mut self, iter: T) -> Result<()> {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        self.validity.reserve(lower);
        self.values.reserve(lower);
        for item in iter {
            let item = item.borrow();
            match item {
                Some(item) => self.push(*item),
                None => self.push_null(),
            }
        }
        Ok(())
    }
}

impl<Ptr: std::borrow::Borrow<Option<bool>>> TryFromIterator<Ptr> for BooleanPrimitive {
    fn try_from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Result<Self> {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity = MutableBitmap::with_capacity(lower);

        let values: MutableBitmap = iter
            .map(|item| {
                Ok(if let Some(a) = item.borrow() {
                    validity.push(true);
                    *a
                } else {
                    validity.push(false);
                    false
                })
            })
            .collect::<Result<_>>()?;

        Ok(Self { values, validity })
    }
}

impl From<BooleanPrimitive> for BooleanArray {
    fn from(p: BooleanPrimitive) -> Self {
        Self::from_data(p.values.into(), p.validity.into())
    }
}

impl IntoArray for BooleanPrimitive {
    fn into_arc(self) -> Arc<dyn Array> {
        let a: BooleanArray = self.into();
        Arc::new(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Array;
    use crate::error::Result;

    #[test]
    fn try_from_iter() -> Result<()> {
        let a = BooleanPrimitive::try_from_iter((0..2).map(|x| Some(x > 0)))?;
        let a: BooleanArray = a.into();
        assert_eq!(a.len(), 2);
        Ok(())
    }
}
