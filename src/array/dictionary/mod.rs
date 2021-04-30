use std::sync::Arc;

use crate::{bitmap::Bitmap, datatypes::DataType, types::NativeType};

use super::{ffi::ToFfi, new_empty_array, primitive::PrimitiveArray, Array};

pub unsafe trait DictionaryKey: NativeType + num::NumCast + num::FromPrimitive {
    const DATA_TYPE: DataType;
}

unsafe impl DictionaryKey for i8 {
    const DATA_TYPE: DataType = DataType::Int8;
}
unsafe impl DictionaryKey for i16 {
    const DATA_TYPE: DataType = DataType::Int16;
}
unsafe impl DictionaryKey for i32 {
    const DATA_TYPE: DataType = DataType::Int32;
}
unsafe impl DictionaryKey for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}
unsafe impl DictionaryKey for u8 {
    const DATA_TYPE: DataType = DataType::UInt8;
}
unsafe impl DictionaryKey for u16 {
    const DATA_TYPE: DataType = DataType::UInt16;
}
unsafe impl DictionaryKey for u32 {
    const DATA_TYPE: DataType = DataType::UInt32;
}
unsafe impl DictionaryKey for u64 {
    const DATA_TYPE: DataType = DataType::UInt64;
}

mod from;
pub use from::*;

#[derive(Debug, Clone)]
pub struct DictionaryArray<K: DictionaryKey> {
    data_type: DataType,
    keys: PrimitiveArray<K>,
    values: Arc<dyn Array>,
    offset: usize,
}

impl<K: DictionaryKey> DictionaryArray<K> {
    pub fn new_empty(data_type: DataType) -> Self {
        let values = new_empty_array(data_type).into();
        Self::from_data(PrimitiveArray::<K>::new_empty(K::DATA_TYPE), values)
    }

    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            PrimitiveArray::<K>::new_null(K::DATA_TYPE, length),
            new_empty_array(data_type).into(),
        )
    }

    pub fn from_data(keys: PrimitiveArray<K>, values: Arc<dyn Array>) -> Self {
        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        Self {
            data_type,
            keys,
            values,
            offset: 0,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone().slice(offset, length),
            values: self.values.clone(),
            offset: self.offset + offset,
        }
    }

    #[inline]
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    #[inline]
    pub fn values(&self) -> &Arc<dyn Array> {
        &self.values
    }
}

impl<K: DictionaryKey> DictionaryArray<K> {
    pub(crate) fn get_child(data_type: &DataType) -> &DataType {
        if let DataType::Dictionary(_, values) = data_type {
            values.as_ref()
        } else {
            panic!("Wrong DataType")
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
        self.keys.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        self.keys.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

impl<K: DictionaryKey> std::fmt::Display for DictionaryArray<K>
where
    PrimitiveArray<K>: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}{{", self.data_type())?;
        writeln!(f, "keys: {},", self.keys())?;
        writeln!(f, "values: {},", self.values())?;
        write!(f, "}}")
    }
}

unsafe impl<K: DictionaryKey> ToFfi for DictionaryArray<K> {
    fn buffers(&self) -> [Option<std::ptr::NonNull<u8>>; 3] {
        [
            self.keys.validity().as_ref().map(|x| x.as_ptr()),
            None,
            None,
        ]
    }

    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}
