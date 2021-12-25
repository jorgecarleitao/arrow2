use std::sync::Arc;

use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, IntegerType},
    scalar::{new_scalar, Scalar},
    types::NativeType,
};

mod ffi;
mod iterator;
mod mutable;
pub use iterator::*;
pub use mutable::*;

use super::display::get_value_display;
use super::{display_fmt, new_empty_array, primitive::PrimitiveArray, Array};
use crate::scalar::NullScalar;

/// Trait denoting [`NativeType`]s that can be used as keys of a dictionary.
pub trait DictionaryKey: NativeType + num_traits::NumCast + num_traits::FromPrimitive {
    /// The corresponding [`IntegerType`] of this key
    const KEY_TYPE: IntegerType;
}

impl DictionaryKey for i8 {
    const KEY_TYPE: IntegerType = IntegerType::Int8;
}
impl DictionaryKey for i16 {
    const KEY_TYPE: IntegerType = IntegerType::Int16;
}
impl DictionaryKey for i32 {
    const KEY_TYPE: IntegerType = IntegerType::Int32;
}
impl DictionaryKey for i64 {
    const KEY_TYPE: IntegerType = IntegerType::Int64;
}
impl DictionaryKey for u8 {
    const KEY_TYPE: IntegerType = IntegerType::UInt8;
}
impl DictionaryKey for u16 {
    const KEY_TYPE: IntegerType = IntegerType::UInt16;
}
impl DictionaryKey for u32 {
    const KEY_TYPE: IntegerType = IntegerType::UInt32;
}
impl DictionaryKey for u64 {
    const KEY_TYPE: IntegerType = IntegerType::UInt64;
}

/// An [`Array`] whose values are encoded by keys. This [`Array`] is useful when the cardinality of
/// values is low compared to the length of the [`Array`].
#[derive(Debug, Clone)]
pub struct DictionaryArray<K: DictionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Arc<dyn Array>,
}

impl<K: DictionaryKey> DictionaryArray<K> {
    /// Returns a new empty [`DictionaryArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let values = Self::get_child(&data_type);
        let values = new_empty_array(values.clone()).into();
        let data_type = K::PRIMITIVE.into();
        Self::from_data(PrimitiveArray::<K>::new_empty(data_type), values)
    }

    /// Returns an [`DictionaryArray`] whose all elements are null
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let values = Self::get_child(&data_type);
        let data_type = K::PRIMITIVE.into();
        Self::from_data(
            PrimitiveArray::<K>::new_null(data_type, length),
            new_empty_array(values.clone()).into(),
        )
    }

    /// The canonical method to create a new [`DictionaryArray`].
    pub fn from_data(keys: PrimitiveArray<K>, values: Arc<dyn Array>) -> Self {
        let data_type =
            DataType::Dictionary(K::KEY_TYPE, Box::new(values.data_type().clone()), false);

        Self {
            data_type,
            keys,
            values,
        }
    }

    /// Creates a new [`DictionaryArray`] by slicing the existing [`DictionaryArray`].
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice(offset, length),
            values: self.values.clone(),
        }
    }

    /// Creates a new [`DictionaryArray`] by slicing the existing [`DictionaryArray`].
    /// # Safety
    /// Safe iff `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice_unchecked(offset, length),
            values: self.values.clone(),
        }
    }

    /// Sets the validity bitmap on this [`Array`].
    /// # Panic
    /// This function panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity should be as least as large as the array")
        }
        let mut arr = self.clone();
        arr.values = Arc::from(arr.values.with_validity(validity));
        arr
    }
}

// accessors
impl<K: DictionaryKey> DictionaryArray<K> {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// The optional validity. Equivalent to `self.keys().validity()`.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.keys.validity()
    }

    /// Returns the keys of the [`DictionaryArray`]. These keys can be used to fetch values
    /// from `values`.
    #[inline]
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    /// Returns the values of the [`DictionaryArray`].
    #[inline]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }

    /// Returns the value of the [`DictionaryArray`] at position `i`.
    #[inline]
    pub fn value(&self, index: usize) -> Box<dyn Scalar> {
        if self.keys.is_null(index) {
            Box::new(NullScalar::new())
        } else {
            let index = self.keys.value(index).to_usize().unwrap();
            new_scalar(self.values.as_ref(), index)
        }
    }
}

impl<K: DictionaryKey> DictionaryArray<K> {
    pub(crate) fn get_child(data_type: &DataType) -> &DataType {
        match data_type {
            DataType::Dictionary(_, values, _) => values.as_ref(),
            DataType::Extension(_, inner, _) => Self::get_child(inner),
            _ => panic!("DictionaryArray must be initialized with DataType::Dictionary"),
        }
    }
}

impl<K: DictionaryKey> Array for DictionaryArray<K> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.keys.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}

impl<K: DictionaryKey> std::fmt::Display for DictionaryArray<K>
where
    PrimitiveArray<K>: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = get_value_display(self);
        let new_lines = false;
        let head = &format!("{:?}", self.data_type());
        let iter = self.iter().enumerate().map(|(i, x)| x.map(|_| display(i)));
        display_fmt(iter, head, f, new_lines)
    }
}
