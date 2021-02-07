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
pub use fixed_binary::FixedSizedBinaryArray;
pub use list::ListArray;
pub use primitive::PrimitiveArray;
pub use specification::Offset;
pub use string::Utf8Array;
pub use struct_::StructArray;
pub use dictionary::{DictionaryArray, DicionaryKey};

pub use self::ffi::FromFFI;
use self::ffi::ToFFI;
