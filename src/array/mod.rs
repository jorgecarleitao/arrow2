use std::any::Any;

use crate::{buffer::Bitmap, datatypes::DataType};

pub trait Array: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn len(&self) -> usize;

    fn data_type(&self) -> &DataType;

    fn nulls(&self) -> &Option<Bitmap>;

    #[inline]
    fn null_count(&self) -> usize {
        self.nulls().as_ref().map(|x| x.null_count()).unwrap_or(0)
    }
}

mod binary;
mod dictionary;
mod fixed_binary;
mod list;
mod primitive;
mod specification;

mod equal;

pub use list::Offset;
pub use primitive::PrimitiveArray;
