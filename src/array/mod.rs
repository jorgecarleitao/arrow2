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
        DataType::Boolean => Box::new(BooleanArray::new_empty()),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::new_empty(data_type)),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            Box::new(PrimitiveArray::<i32>::new_empty(data_type))
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) => {
            Box::new(PrimitiveArray::<i64>::new_empty(data_type))
        }
        _ => unimplemented!(),
    }
}

mod binary;
mod boolean;
mod dictionary;
mod fixed_binary;
mod list;
mod primitive;
mod specification;
mod string;
mod struct_;

mod equal;
mod ffi;

pub use binary::BinaryArray;
pub use boolean::BooleanArray;
pub use dictionary::{DicionaryKey, DictionaryArray};
pub use fixed_binary::FixedSizedBinaryArray;
pub use list::ListArray;
pub use primitive::PrimitiveArray;
pub use specification::Offset;
pub use string::Utf8Array;
pub use struct_::StructArray;

pub use self::ffi::FromFFI;
use self::ffi::ToFFI;
