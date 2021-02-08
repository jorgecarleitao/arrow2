use std::any::Any;

use crate::{buffer::Bitmap, datatypes::DataType};

pub trait Array: std::fmt::Debug + Send + Sync + ToFFI {
    fn as_any(&self) -> &dyn Any;

    fn len(&self) -> usize;

    fn data_type(&self) -> &DataType;

    fn nulls(&self) -> &Option<Bitmap>;

    #[inline]
    fn null_count(&self) -> usize {
        self.nulls().as_ref().map(|x| x.null_count()).unwrap_or(0)
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls()
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array>;
}

/// Creates a new empty dynamic array
pub fn new_empty_array(data_type: DataType) -> Box<dyn Array> {
    match data_type {
        DataType::Null => Box::new(NullArray::new_empty()),
        DataType::Boolean => Box::new(BooleanArray::new_empty()),
        DataType::Int8 => Box::new(PrimitiveArray::<i8>::new_empty(data_type)),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::new_empty(data_type)),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            Box::new(PrimitiveArray::<i32>::new_empty(data_type))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Box::new(PrimitiveArray::<i64>::new_empty(data_type)),
        DataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_empty(data_type)),
        DataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_empty(data_type)),
        DataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_empty(data_type)),
        DataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_empty(data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(PrimitiveArray::<f32>::new_empty(data_type)),
        DataType::Float64 => Box::new(PrimitiveArray::<f64>::new_empty(data_type)),
        DataType::Binary => Box::new(BinaryArray::<i32>::new_empty()),
        DataType::LargeBinary => Box::new(BinaryArray::<i64>::new_empty()),
        DataType::FixedSizeBinary(size) => Box::new(FixedSizedBinaryArray::new_empty(size)),
        DataType::Utf8 => Box::new(Utf8Array::<i32>::new_empty()),
        DataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_empty()),
        DataType::List(field) => Box::new(ListArray::<i32>::new_empty(&field)),
        DataType::LargeList(field) => Box::new(ListArray::<i64>::new_empty(&field)),
        DataType::FixedSizeList(_, _) => unimplemented!(),
        DataType::Struct(fields) => Box::new(StructArray::new_empty(&fields)),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => Box::new(DictionaryArray::<i8>::new_empty(*value_type)),
            DataType::Int16 => Box::new(DictionaryArray::<i16>::new_empty(*value_type)),
            DataType::Int32 => Box::new(DictionaryArray::<i32>::new_empty(*value_type)),
            DataType::Int64 => Box::new(DictionaryArray::<i64>::new_empty(*value_type)),
            DataType::UInt8 => Box::new(DictionaryArray::<u8>::new_empty(*value_type)),
            DataType::UInt16 => Box::new(DictionaryArray::<u16>::new_empty(*value_type)),
            DataType::UInt32 => Box::new(DictionaryArray::<u32>::new_empty(*value_type)),
            DataType::UInt64 => Box::new(DictionaryArray::<u64>::new_empty(*value_type)),
            _ => unreachable!(),
        },
        DataType::Decimal(_, _) => unimplemented!(),
    }
}

mod binary;
mod boolean;
mod dictionary;
mod fixed_binary;
mod list;
mod null;
mod primitive;
mod specification;
mod string;
mod struct_;

mod equal;
mod ffi;

pub use binary::BinaryArray;
pub use boolean::BooleanArray;
pub use dictionary::{DictionaryArray, DictionaryKey};
pub use fixed_binary::FixedSizedBinaryArray;
pub use list::ListArray;
pub use null::NullArray;
pub use primitive::PrimitiveArray;
pub use specification::Offset;
pub use string::Utf8Array;
pub use struct_::StructArray;

pub use self::ffi::FromFFI;
use self::ffi::ToFFI;
